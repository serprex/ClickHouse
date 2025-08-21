#include <AggregateFunctions/AggregateFunctionDeepMergeJSON.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int TOO_LARGE_ARRAY_SIZE;
extern const int TOO_LARGE_STRING_SIZE;
}

namespace
{

/*
SerializationPtr getVariantSerialization(const DataTypePtr & variant_type)
{
    return variant_type->getDefaultSerialization();
}
*/

const FormatSettings & getFormatSettings()
{
    static const FormatSettings settings;
    return settings;
}

const std::shared_ptr<SerializationDynamic> & getDynamicSerialization()
{
    static const std::shared_ptr<SerializationDynamic> dynamic_serialization = std::make_shared<SerializationDynamic>();
    return dynamic_serialization;
}

/// Helper to validate path length
void validatePathLength(size_t path_size)
{
    if (path_size > MAX_JSON_MERGE_PATH_LENGTH)
        throw Exception(
            ErrorCodes::TOO_LARGE_STRING_SIZE, "JSON path too long: {} bytes (maximum: {})", path_size, MAX_JSON_MERGE_PATH_LENGTH);
}

/// Helper to validate paths count
void validatePathsCount(size_t paths_count)
{
    if (paths_count > MAX_JSON_MERGE_PATHS)
        throw Exception(
            ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too many paths in JSON merge: {} (maximum: {})", paths_count, MAX_JSON_MERGE_PATHS);
}

/// Helper to validate total size
void validateTotalSize(size_t total_size)
{
    if (total_size > MAX_JSON_MERGE_TOTAL_SIZE)
        throw Exception(
            ErrorCodes::TOO_LARGE_STRING_SIZE,
            "JSON merge state size too large: {} bytes (maximum: {} bytes)",
            total_size,
            MAX_JSON_MERGE_TOTAL_SIZE);
}
}

bool DeepMergeJSONAggregateData::isObjectPath(const StringRef & path) const
{
    if (auto it = typed_paths.upper_bound(path); it != typed_paths.end()) {
        return it->first.size > path.size && memcmp(it->first.data, path.data, path.size) == 0 && it->first.data[path.size] == '.';
    }
    if (auto it = dynamic_paths.upper_bound(path); it != dynamic_paths.end()) {
        return it->first.size > path.size && memcmp(it->first.data, path.data, path.size) == 0 && it->first.data[path.size] == '.';
    }
    return false;
}

void DeepMergeJSONAggregateData::addTypedPath(const StringRef & path, const Field & value, Arena *)
{
    auto it = typed_paths.find(path);
    if (it != typed_paths.end())
    {
        it->second = value;
    }
    else
    {
        typed_paths[path] = value;
    }

    /// Remove child paths if this is now a leaf value (non-object)
    if (!value.isNull() && value.getType() != Field::Types::Object)
        removeChildPaths(path);
}

void DeepMergeJSONAggregateData::removeChildPaths(const StringRef & parent_path)
{
    String prefix = parent_path.toString() + ".";

    auto typed_it = typed_paths.lower_bound(StringRef(prefix));
    while (typed_it != typed_paths.end() && typed_it->first.size >= prefix.size() && memcmp(typed_it->first.data, prefix.data(), prefix.size()) == 0)
    {
        typed_it = typed_paths.erase(typed_it);
    }

    auto dynamic_it = dynamic_paths.lower_bound(StringRef(prefix));
    while (dynamic_it != dynamic_paths.end() && dynamic_it->first.size >= prefix.size() && memcmp(dynamic_it->first.data, prefix.data(), prefix.size()) == 0)
    {
        dynamic_it = dynamic_paths.erase(dynamic_it);
    }
}

void AggregateFunctionDeepMergeJSON::add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const
{
    auto & aggregate_data = data(place);
    const auto & col_object = assert_cast<const ColumnObject &>(*columns[0]);

    processColumnObject(col_object, row_num, aggregate_data, arena);
}

void AggregateFunctionDeepMergeJSON::processColumnObject(
    const ColumnObject & col_object, size_t row_num, DeepMergeJSONAggregateData & aggregate_data, Arena * arena) const
{
    /// Process typed paths
    for (const auto & [path, column] : col_object.getTypedPaths())
    {
        Field value;
        column->get(row_num, value);
        validatePathLength(path.size());
        auto interned_path = internString(path, arena);
        aggregate_data.addTypedPath(interned_path, value, arena);
    }

    /// Process dynamic paths
    for (const auto & [path, dynamic_column] : col_object.getDynamicPathsPtrs())
    {
        if (!dynamic_column->isNullAt(row_num))
        {
            auto interned_path = internString(path, arena);
            WriteBufferFromOwnString value_buf;
            getDynamicSerialization()->serializeBinary(dynamic_column, value_buf, getFormatSettings());
            auto value_view = value_buf.stringView();
            StringRef value_ref(value_view);
            aggregate_data.dynamic_paths[interned_path] = internString(value_ref, arena);
        }
    }

    /// Process shared data
    const auto [shared_data_paths, shared_data_values] = col_object.getSharedDataPathsAndValues();
    const auto & shared_data_offsets = col_object.getSharedDataOffsets();
    size_t start = shared_data_offsets[static_cast<ssize_t>(row_num) - 1];
    size_t end = shared_data_offsets[static_cast<ssize_t>(row_num)];

    for (size_t i = start; i < end; ++i)
    {
        auto path = shared_data_paths->getDataAt(i);
        validatePathLength(path.size);

        auto interned_path = internString(path, arena);
        auto value_data = shared_data_values->getDataAt(i);
        auto interned_value = internString(value_data, arena);
        aggregate_data.dynamic_paths[interned_path] = internString(interned_value, arena);
    }

    validatePathsCount(aggregate_data.typed_paths.size());
    validatePathsCount(aggregate_data.dynamic_paths.size());
}

void AggregateFunctionDeepMergeJSON::merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const
{
    auto & aggregate_data = data(place);
    const auto & rhs_data = data(rhs);

    /// Merge paths from rhs, treating them as latest values
    for (const auto & [path, path_data] : rhs_data.typed_paths)
    {
        auto interned_path = internString(path, arena);
        aggregate_data.addTypedPath(interned_path, path_data, arena);
    }

    for (const auto & [path, value] : rhs_data.dynamic_paths)
    {
        auto interned_path = internString(path, arena);
        auto interned_value = internString(value, arena);
        aggregate_data.dynamic_paths[interned_path] = interned_value;
    }

    validatePathsCount(aggregate_data.typed_paths.size());
    validatePathsCount(aggregate_data.dynamic_paths.size());
}

void AggregateFunctionDeepMergeJSON::serialize(
    ConstAggregateDataPtr __restrict place, WriteBuffer & buf, [[maybe_unused]] std::optional<size_t> version) const
{
    const auto & aggregate_data = data(place);

    size_t total_size = 0;

    writeVarUInt(aggregate_data.typed_paths.size(), buf);

    for (const auto & [path, path_data] : aggregate_data.typed_paths)
    {
        writeStringBinary(path, buf);

        /// Serialize Field
        WriteBufferFromOwnString field_buf;
        writeFieldBinary(path_data, field_buf);
        writeStringBinary(field_buf.str(), buf);

        total_size += path.size + field_buf.str().size();
        validateTotalSize(total_size);
    }

    writeVarUInt(aggregate_data.dynamic_paths.size(), buf);

    for (const auto & [path, path_data] : aggregate_data.dynamic_paths)
    {
        writeStringBinary(path, buf);
        writeStringBinary(path_data, buf);

        total_size += path.size + path_data.size;
        validateTotalSize(total_size);
    }
}

void AggregateFunctionDeepMergeJSON::deserialize(
    AggregateDataPtr __restrict place, ReadBuffer & buf, [[maybe_unused]] std::optional<size_t> version, Arena * arena) const
{
    auto & aggregate_data = data(place);
    aggregate_data.typed_paths.clear();
    aggregate_data.dynamic_paths.clear();

    size_t total_size = 0;
    size_t num_paths;

    readVarUInt(num_paths, buf);
    validatePathsCount(num_paths);
    for (size_t i = 0; i < num_paths; ++i)
    {
        String path_str;
        readStringBinary(path_str, buf);
        validatePathLength(path_str.size());

        String value_str;
        readStringBinary(value_str, buf);

        total_size += path_str.size() + value_str.size();
        validateTotalSize(total_size);

        /// Deserialize Field
        ReadBufferFromString value_buf(value_str);
        Field value = readFieldBinary(value_buf);

        auto interned_path = internString(path_str, arena);
        aggregate_data.typed_paths[interned_path] = value;
    }

    readVarUInt(num_paths, buf);
    validatePathsCount(num_paths);
    for (size_t i = 0; i < num_paths; ++i)
    {
        String path_str;
        readStringBinary(path_str, buf);
        validatePathLength(path_str.size());

        String value_str;
        readStringBinary(value_str, buf);

        total_size += path_str.size() + value_str.size();
        validateTotalSize(total_size);

        auto interned_path = internString(path_str, arena);
        auto interned_value = internString(value_str, arena);
        aggregate_data.dynamic_paths[interned_path] = interned_value;
    }
}

void AggregateFunctionDeepMergeJSON::insertResultInto(AggregateDataPtr __restrict place, IColumn & to, [[maybe_unused]] Arena * arena) const
{
    const auto & aggregate_data = data(place);
    auto & col_object = assert_cast<ColumnObject &>(to);
    auto typed = col_object.getTypedPaths();
    auto dynamic = col_object.getDynamicPaths();
    auto & shared_data_offsets = col_object.getSharedDataOffsets();
    const auto [shared_data_paths, shared_data_values] = col_object.getSharedDataPathsAndValues();
    size_t col_current_size = col_object.size();

    for (const auto & [path, field] : aggregate_data.typed_paths)
    {
        if (auto typed_it = typed.find(path); typed_it != typed.end())
        {
            typed_it->second->insert(field);
        }
        else if (auto dynamic_it = dynamic.find(path); dynamic_it != dynamic.end())
        {
            dynamic_it->second->insert(field);
        }
        else if (auto * dynamic_path_column = col_object.tryToAddNewDynamicPath(std::string_view(path)))
        {
            dynamic_path_column->insert(field);
        }
        /// We reached the limit on dynamic paths. Add this path to the common data if the value is not Null.
        /// (we cannot distinguish cases when path has Null value or is absent in the row and consider them equivalent).
        /// Object is actually std::map, so all paths are already sorted and we can add it right now.
        else if (!field.isNull())
        {
            shared_data_paths->insertData(path.data, path.size);
            auto & shared_data_values_chars = shared_data_values->getChars();
            {
                WriteBufferFromVector<ColumnString::Chars> value_buf(shared_data_values_chars, AppendModeTag());
                getDynamicSerialization()->serializeBinary(field, value_buf, getFormatSettings());
            }
            shared_data_values_chars.push_back(0);
            shared_data_values->getOffsets().push_back(shared_data_values_chars.size());
        }
    }

    for (const auto & [path, value] : aggregate_data.dynamic_paths)
    {
        /// Check if we have this path in dynamic paths.
        if (auto dynamic_it = dynamic.find(path); dynamic_it != dynamic.end())
        {
            ReadBufferFromMemory buf(value.data, value.size);
            getDynamicSerialization()->deserializeBinary(*dynamic_it->second, buf, getFormatSettings());
        }
        /// Try to add a new dynamic path.
        else if (auto * dynamic_path_column = col_object.tryToAddNewDynamicPath(std::string_view(path)))
        {
            ReadBufferFromMemory buf(value.data, value.size);
            getDynamicSerialization()->deserializeBinary(*dynamic_path_column, buf, getFormatSettings());
        }
        /// Limit on dynamic paths is reached, add this path to shared data.
        /// Serialized paths are sorted, so we can insert right away.
        else
        {
            shared_data_paths->insertData(path.data, path.size);
            //shared_data_values->insertData(value.data, value.size);
            auto & shared_data_values_chars = shared_data_values->getChars();
            {
                WriteBufferFromVector<ColumnString::Chars> value_buf(shared_data_values_chars, AppendModeTag());
                value_buf.write(value.data, value.size);
            }
            shared_data_values_chars.push_back(0);
            shared_data_values->getOffsets().push_back(shared_data_values_chars.size());
        }
    }

    shared_data_offsets.push_back(shared_data_paths->size());
    /// Fill all remaining typed and dynamic paths with default values.
    for (auto & [_, column] : typed)
    {
        if (column->size() == col_current_size)
            column->insertDefault();
    }

    for (auto & [_, column] : dynamic)
    {
        if (column->size() == col_current_size)
            column->insertDefault();
    }
}

void AggregateFunctionDeepMergeJSON::addBatchSinglePlace(
    size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos)
    const
{
    if (if_argument_pos >= 0)
    {
        IAggregateFunctionDataHelper<DeepMergeJSONAggregateData, AggregateFunctionDeepMergeJSON>::addBatchSinglePlace(
            row_begin, row_end, place, columns, arena, if_argument_pos);
        return;
    }

    const auto & col_object = assert_cast<const ColumnObject &>(*columns[0]);
    auto & aggregate_data = data(place);

    for (size_t row = row_begin; row < row_end; ++row)
    {
        processColumnObject(col_object, row, aggregate_data, arena);
    }
}

void AggregateFunctionDeepMergeJSON::addManyDefaults(
    AggregateDataPtr __restrict /*place*/, const IColumn ** /*columns*/, size_t /*length*/, Arena * /*arena*/) const
{
    /// Default value for JSON is empty object, so nothing to add
}

namespace
{

AggregateFunctionPtr
createAggregateFunctionDeepMergeJSON(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly one argument", name);

    return std::make_shared<AggregateFunctionDeepMergeJSON>(argument_types, parameters);
}

}

void registerAggregateFunctionDeepMergeJSON(AggregateFunctionFactory & factory)
{
    factory.registerFunction("deepMergeJSON", createAggregateFunctionDeepMergeJSON);
}

}
