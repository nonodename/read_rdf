#define DUCKDB_EXTENSION_MAIN

#include "read_rdf_extension.hpp"
#include "duckdb.hpp"
#include "include/serd_buffer.hpp"
#include "include/xml_buffer.hpp"
#include "include/I_triples_buffer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include "duckdb/common/file_system.hpp"

using namespace std;

#define STRICT_PARSING   "strict_parsing"
#define PREFIX_EXPANSION "prefix_expansion"

namespace duckdb {

static bool isRDFXML(const std::string &path) {
	auto pos = path.rfind('.');
	if (pos == std::string::npos)
		return false;
	std::string ext = path.substr(pos + 1);
	for (auto &c : ext)
		c = (char)tolower(c);
	return (ext == "rdf" || ext == "xml");
}

struct RDFReaderBindData : public TableFunctionData {
	string file_path;
	string file_type;
	string baseURI;
	bool strict_parsing = true;
	bool expand_prefixes = false;
};

struct RDFReaderLocalState : public LocalTableFunctionState {
	std::unique_ptr<ITriplesBuffer> ib;
};

static unique_ptr<FunctionData> RDFReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RDFReaderBindData>();
	// TODO
	// Check input count
	// Permit caller to override file type, baseURI etc.
	// Permit caller to pass in multiple files (e.g. for sharded RDF data)
	// Permit caller to expand prefixed values
	result->file_path = input.inputs[0].GetValue<string>();
	auto strict_parsing_param = input.named_parameters.find(STRICT_PARSING);
	if (strict_parsing_param != input.named_parameters.end()) {
		result->strict_parsing = strict_parsing_param->second.GetValue<bool>();
	} else {
		result->strict_parsing = true;
	}
	auto prefix_expansion_param = input.named_parameters.find(PREFIX_EXPANSION);
	if (prefix_expansion_param != input.named_parameters.end()) {
		result->expand_prefixes = prefix_expansion_param->second.GetValue<bool>();
	} else {
		result->expand_prefixes = false;
	}
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
	auto _ib = unique_ptr<ITriplesBuffer>(nullptr);
	if (isRDFXML(bind_data.file_path)) {
		_ib = make_uniq<XMLBuffer>(bind_data.file_path, "", bind_data.strict_parsing, bind_data.expand_prefixes);
	} else {
		_ib = make_uniq<SerdBuffer>(bind_data.file_path, "", bind_data.strict_parsing, bind_data.expand_prefixes);
	}
	try {
		_ib->StartParse();
	} catch (const std::runtime_error &re) {
		cerr << "Exception in RDFReaderInit: " << re.what() << "\n";
		throw IOException(re.what());
	}
	state->ib = std::move(_ib);
	return state;
}

static void RDFReaderFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = (RDFReaderLocalState &)*input.local_state;
	// Delegate the filling entirely to the buffer
	state.ib->PopulateChunk(output);
}

static void LoadInternal(ExtensionLoader &loader) {
	string extension_name = "read_rdf";
	TableFunction tf(extension_name, {LogicalType::VARCHAR}, RDFReaderFunc, RDFReaderBind, nullptr, RDFReaderInit);
	tf.named_parameters[STRICT_PARSING] = LogicalType::BOOLEAN;
	tf.named_parameters[PREFIX_EXPANSION] = LogicalType::BOOLEAN;
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
	return "0.0.1-unknown";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(read_rdf, loader) {
	duckdb::LoadInternal(loader);
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
