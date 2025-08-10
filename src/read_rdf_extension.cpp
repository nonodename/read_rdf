#define DUCKDB_EXTENSION_MAIN

#include "read_rdf_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include "duckdb/common/file_system.hpp"

// OpenSSL linked through vcpkg
#include <fstream>
#include <regex>
using namespace std;
namespace duckdb {

struct RDFReaderBindData : public TableFunctionData {
	string file_path;
};

struct RDFReaderLocalState : public LocalTableFunctionState {
	ifstream file;
	idx_t row_idx;
};

// Enhanced parser to extract language tag or datatype
bool ParseTripleLine(const string &line, string &subject, string &predicate, string &object, string &lang_tag,
                     string &datatype_iri) {
	static regex triple_regex(
        R"(^\s*<([^>]*)>\s+<([^>]*)>\s+(\"[^\"]*\"(?:@[a-zA-Z\-]+|\^\^<[^>]+>)?|<[^>]*>|_:.*)\s*\.\s*$)");

	smatch match;
	if (!regex_match(line, match, triple_regex) || match.size() != 4)
		return false;

	subject = match[1].str();
	predicate = match[2].str();
	string obj = match[3].str();

	lang_tag.clear();
	datatype_iri.clear();

	if (obj[0] == '<') {
		object = obj.substr(1, obj.size() - 2); // strip <...>
	} else if (obj[0] == '"') {
		size_t end_quote = obj.find_last_of('"');
		object = obj.substr(1, end_quote - 1); // extract literal value

		size_t tag_start = end_quote + 1;
		if (tag_start < obj.size()) {
			if (obj[tag_start] == '@') {
				lang_tag = obj.substr(tag_start + 1); // @en
			} else if (obj.compare(tag_start, 2, "^^") == 0) {
				size_t iri_start = obj.find('<', tag_start);
				size_t iri_end = obj.find('>', iri_start);
				if (iri_start != string::npos && iri_end != string::npos) {
					datatype_iri = obj.substr(iri_start + 1, iri_end - iri_start - 1);
				}
			}
		}
	} else if (obj.substr(0, 2) == "_:") {
		object = obj;
	} else {
		return false;
	}

	return true;
}


static unique_ptr<FunctionData> RDFReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<RDFReaderBindData>();
    result->file_path = input.inputs[0].GetValue<string>();

    return_types = {
        LogicalType::VARCHAR,  // subject
        LogicalType::VARCHAR,  // predicate
        LogicalType::VARCHAR,  // object
        LogicalType::VARCHAR,  // language_tag
        LogicalType::VARCHAR   // datatype_iri
    };
    names = {"subject", "predicate", "object", "language_tag", "datatype_iri"};

    return std::move(result);
}

static unique_ptr<LocalTableFunctionState> RDFReaderInit(
    ExecutionContext &context,
    TableFunctionInitInput &input,
    GlobalTableFunctionState *global_state
) {
    auto &bind_data = (RDFReaderBindData &)*input.bind_data;
    auto state = make_uniq<RDFReaderLocalState>();
    state->file.open(bind_data.file_path);
    state->row_idx = 0;

    if (!state->file.is_open()) {
        throw IOException("Could not open RDF file: " + bind_data.file_path);
    }

    return state;
}

static void RDFReaderFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &state = (RDFReaderLocalState &)*input.local_state;

    idx_t count = 0;
    string line;
    string subject, predicate, object, lang_tag, datatype_iri;

    while (count < STANDARD_VECTOR_SIZE && std::getline(state.file, line)) {
        StringUtil::Trim(line);
        if (line.empty() || line[0] == '#') {
            continue;
        }

        if (!ParseTripleLine(line, subject, predicate, object, lang_tag, datatype_iri)) {
            continue; // skip malformed lines
        }

        output.SetValue(0, count, Value(subject));
        output.SetValue(1, count, Value(predicate));
        output.SetValue(2, count, Value(object));
        output.SetValue(3, count, lang_tag.empty() ? Value() : Value(lang_tag));
        output.SetValue(4, count, datatype_iri.empty() ? Value() : Value(datatype_iri));
        count++;
    }

    output.SetCardinality(count);
}

static void LoadInternal(DatabaseInstance &instance) {
	string extension_name = "read_rdf";
    TableFunction tf(extension_name, {LogicalType::VARCHAR}, RDFReaderFunc, RDFReaderBind, nullptr, RDFReaderInit);
    ExtensionUtil::RegisterFunction(instance, tf);
}

void ReadRdfExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string ReadRdfExtension::Name() {
	return "read_rdf";
}

std::string ReadRdfExtension::Version() const {
#ifdef EXT_VERSION_READ_RDF
	return EXT_VERSION_READ_RDF;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void read_rdf_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::ReadRdfExtension>();
}

DUCKDB_EXTENSION_API const char *read_rdf_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
