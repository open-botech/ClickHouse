#include <Processors/Formats/Impl/JSONAsStringEachRowInputFormat.h>
#include <Formats/JSONEachRowUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <base/find_symbols.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

JSONAsStringEachRowInputFormat::JSONAsStringEachRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_) :
    IRowInputFormat(header_, in_, std::move(params_))
{
    if (header_.columns() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "This input format is only suitable for tables with a single column of type String but the number of columns is {}",
                        header_.columns());

    if (!isString(removeNullable(removeLowCardinality(header_.getByPosition(0).type))))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "This input format is only suitable for tables with a single column of type String but the column type is {}",
                        header_.getByPosition(0).type->getName());
}
void JSONAsStringEachRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(*in);
}

void JSONAsStringEachRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
}

void JSONAsStringEachRowInputFormat::readJSONObject(IColumn & column)
{
    size_t balance = 0;
    bool quotes = false;
    char * start = in->position();
    if (*in->position() != '{')
        throw Exception("JSON object must begin with '{'.", ErrorCodes::INCORRECT_DATA);

    ++in->position();
    ++balance;

    char * pos;

    while (balance)
    {
        if (in->eof())
            throw Exception("Unexpected end of file while parsing JSON object.", ErrorCodes::INCORRECT_DATA);

        if (quotes)
        {
            pos = find_first_symbols<'"', '\\'>(in->position(), in->buffer().end());
            in->position() = pos;
            if (in->position() == in->buffer().end())
                continue;
            if (*in->position() == '"')
            {
                quotes = false;
                ++in->position();
            }
            else if (*in->position() == '\\')
            {
                ++in->position();
                if (!in->eof())
                {
                    ++in->position();
                }
            }
        }
        else
        {
            pos = find_first_symbols<'"', '{', '}', '\\'>(in->position(), in->buffer().end());
            in->position() = pos;
            if (in->position() == in->buffer().end())
                continue;
            if (*in->position() == '{')
            {
                ++balance;
                ++in->position();
            }
            else if (*in->position() == '}')
            {
                --balance;
                ++in->position();
            }
            else if (*in->position() == '\\')
            {
                ++in->position();
                if (!in->eof())
                {
                    ++in->position();
                }
            }
            else if (*in->position() == '"')
            {
                quotes = true;
                ++in->position();
            }
        }
    }
    column.insertData(start, in->position() - start);

}

bool JSONAsStringEachRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    skipWhitespaceIfAny(*in);

    if (!in->eof())
        readJSONObject(*columns[0]);

    skipWhitespaceIfAny(*in);
    if (!in->eof() && *in->position() == ',')
        ++in->position();
    skipWhitespaceIfAny(*in);

    return !in->eof();
}

void registerInputFormatJSONAsStringEachRow(FormatFactory & factory)
{
    factory.registerInputFormat("JSONAsStringEachRow", [](
                                                                    ReadBuffer & buf,
                                                                    const Block & sample,
                                                                    const RowInputFormatParams & params,
                                                                    const FormatSettings &)
                                         {
                                             return std::make_shared<JSONAsStringEachRowInputFormat>(sample, buf, params);
                                         });
}

void registerFileSegmentationEngineJSONAsStringEachRow(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("JSONAsStringEachRow", &fileSegmentationEngineJSONEachRowImpl);
}

}
