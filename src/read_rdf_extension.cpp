#define DUCKDB_EXTENSION_MAIN

#include "read_rdf_extension.hpp"
#include "duckdb.hpp"
#include "include/serd_buffer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include "duckdb/common/file_system.hpp"

using namespace std;

namespace duckdb {

struct RDFReaderBindData : public TableFunctionData {
	string file_path;
	string file_type;
	string baseURI;
};

struct RDFReaderLocalState : public LocalTableFunctionState {
	std::unique_ptr<SerdBuffer> sb;
};

static unique_ptr<FunctionData> RDFReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RDFReaderBindData>();
	result->file_path = input.inputs[0].GetValue<string>();
	auto &fs = FileSystem::GetFileSystem(context);
	string expanded = fs.ExpandPath(result->file_path);
	string normalized = fs.NormalizeAbsolutePath(expanded);
	names = {"graph", "subject", "predicate", "object", "object_datatype", "object_lang"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(result);
}
static unique_ptr<LocalTableFunctionState> RDFReaderInit(ExecutionContext &context, TableFunctionInitInput &input,
                                                         GlobalTableFunctionState *global_state) {
	auto &bind_data = (RDFReaderBindData &)*input.bind_data;
	auto state = make_uniq<RDFReaderLocalState>();
	auto _sb = make_uniq<SerdBuffer>(bind_data.file_path, "");
	try {
		_sb->StartParse();
	} catch (const std::runtime_error &re) {
		cerr << "Exception in RDFReaderInit: " << re.what() << "\n";
		throw IOException(re.what());
	}
	state->sb = std::move(_sb);
	return state;
}

static void RDFReaderFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = (RDFReaderLocalState &)*input.local_state;
	auto &parser = *state.sb;

	idx_t count = 0;
	const idx_t target = STANDARD_VECTOR_SIZE; // fill full chunk for throughput

	while (count < target) {
		try {
			if (parser.EverythingProcessed()) {
				break; // EOF and no rows available
			}
			RDFRow row = parser.GetNextRow();
			output.SetValue(0, count, Value(row.graph));
			output.SetValue(1, count, Value(row.subject));
			output.SetValue(2, count, Value(row.predicate));
			output.SetValue(3, count, Value(row.object));
			output.SetValue(4, count, Value(row.datatype));
			output.SetValue(5, count, Value(row.lang));
		} catch (const std::runtime_error &error) {
			string s = error.what();
			throw SyntaxException(s);
		}
		count++;
	}
	output.SetCardinality(count);
}

static void LoadInternal(ExtensionLoader &loader) {
	string extension_name = "read_rdf";
	TableFunction tf(extension_name, {LogicalType::VARCHAR}, RDFReaderFunc, RDFReaderBind, nullptr, RDFReaderInit);
	loader.RegisterFunction(tf);
}

void ReadRdfExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
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

DUCKDB_CPP_EXTENSION_ENTRY(read_rdf, loader) {
	duckdb::LoadInternal(loader);
}
/*DUCKDB_EXTENSION_API void read_rdf_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::ReadRdfExtension>();
}

DUCKDB_EXTENSION_API const char *read_rdf_version() {
    return duckdb::DuckDB::LibraryVersion();
}*/
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
