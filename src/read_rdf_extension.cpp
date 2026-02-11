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
#define FILE_TYPE        "file_type"

namespace duckdb {

static ITriplesBuffer::FileType ConvertLabelToFileType(const std::string &s) {
	std::string x = s;
	for (auto &c : x)
		c = (char)tolower(c);
	if (x == "ttl" || x == "turtle")
		return ITriplesBuffer::TURTLE;
	if (x == "nq" || x == "nquads")
		return ITriplesBuffer::NQUADS;
	if (x == "nt" || x == "ntriples")
		return ITriplesBuffer::NTRIPLES;
	if (x == "trig")
		return ITriplesBuffer::TRIG;
	if (x == "rdf" || x == "xml")
		return ITriplesBuffer::XML;
	return ITriplesBuffer::UNKNOWN;
}

static ITriplesBuffer::FileType DetectFileTypeFromPath(const std::string &path) {
	auto pos = path.rfind('.');
	if (pos == std::string::npos)
		return ITriplesBuffer::UNKNOWN;
	std::string ext = path.substr(pos + 1);
	return ConvertLabelToFileType(ext);
}

static ITriplesBuffer::FileType ParseFileTypeString(const std::string &s) {
	ITriplesBuffer::FileType ft = ConvertLabelToFileType(s);
	if (ft == ITriplesBuffer::UNKNOWN)
		throw std::runtime_error("Unknown file_type override: '" + s + "'");
	return ft;
}

struct RDFReaderBindData : public TableFunctionData {
	string file_path;
	ITriplesBuffer::FileType file_type = ITriplesBuffer::TURTLE;
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
	// Permit caller to pass in multiple files (e.g. for sharded RDF data)
	auto &fs = FileSystem::GetFileSystem(context);
	string expanded = fs.ExpandPath(input.inputs[0].GetValue<string>());
	string normalized = fs.NormalizeAbsolutePath(expanded);
	result->file_path = normalized;
	// Optional override for file type (e.g. "ttl", "nt", "nq", "trig", "xml")
	auto file_type_param = input.named_parameters.find(FILE_TYPE);
	if (file_type_param != input.named_parameters.end()) {
		result->file_type = ParseFileTypeString(file_type_param->second.GetValue<string>());
	} else {
		result->file_type = DetectFileTypeFromPath(result->file_path);
	}
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
	switch (bind_data.file_type) {
	case ITriplesBuffer::TURTLE:
	case ITriplesBuffer::NQUADS:
	case ITriplesBuffer::NTRIPLES:
	case ITriplesBuffer::TRIG:
		_ib = make_uniq<SerdBuffer>(bind_data.file_path, "", bind_data.strict_parsing, bind_data.expand_prefixes,
		                            bind_data.file_type);
		break;
	case ITriplesBuffer::XML:
		_ib = make_uniq<XMLBuffer>(bind_data.file_path, "", bind_data.strict_parsing, bind_data.expand_prefixes,
		                           bind_data.file_type);
		break;
	default:
		throw std::runtime_error("Unknown file type for: " + bind_data.file_path);
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
	tf.named_parameters[FILE_TYPE] = LogicalType::VARCHAR;
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
