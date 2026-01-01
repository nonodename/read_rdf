#include "include/xml_buffer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"

#define PARSING_CHUNK_SIZE 4096

XMLBuffer::XMLBuffer( std::string path,  std::string base_uri, const bool strict_parsing,
                     const bool expand_prefixes)
    : ITriplesBuffer(path, base_uri, strict_parsing, expand_prefixes),
      _parser([this](const RdfStatement &s) { this->statementCallback(s); },
              [this](std::string_view prefix, std::string_view uri) { this->namespaceCallback(prefix, uri); },
              [this](const std::string &msg) { this->errorCallback(msg); }, base_uri) {

	std::cerr << "Opening XML file_path: " << _file_path << std::endl;
	std::cout << "Opening XML file: " << path << std::endl;

	FILE *_f = std::fopen(_file_path.c_str(), "rb");
	if (!_f) {
		throw std::runtime_error("Could not open RDF file: " + _file_path);
	}

	_file.reset(_f);
}
XMLBuffer::~XMLBuffer() {
}

void XMLBuffer::PopulateChunk(duckdb::DataChunk &output) {
	_current_chunk = &output;
	_current_count = 0;
	while (!_overflow_buffer.empty() && _current_count < STANDARD_VECTOR_SIZE) {
		RDFRow row = _overflow_buffer.front();
		_overflow_buffer.pop_front();
		// Manual copy from string to vector (slow path)
		output.SetValue(0, _current_count, duckdb::Value(row.graph));
		output.SetValue(1, _current_count, duckdb::Value(row.subject));
		output.SetValue(2, _current_count, duckdb::Value(row.predicate));
		output.SetValue(3, _current_count, duckdb::Value(row.object));
		output.SetValue(4, _current_count, duckdb::Value(row.datatype));
		output.SetValue(5, _current_count, duckdb::Value(row.lang));
		_current_count++;
	}

	char buffer[PARSING_CHUNK_SIZE];
	while (_current_count < STANDARD_VECTOR_SIZE && !_eof) {
		int res = fread(buffer, 1, PARSING_CHUNK_SIZE, _file.get());
		_eof = (res < PARSING_CHUNK_SIZE);
		_parser.parseChunk(buffer, res, _eof);
	}
	output.SetCardinality(_current_count);
	_current_chunk = nullptr;
}
void XMLBuffer::StartParse() {
}

void XMLBuffer::writeToVector(duckdb::Vector &vec, idx_t row_idx, const std::string &field) {
	if (field.empty()) {
		duckdb::FlatVector::SetNull(vec, row_idx, true);
		return;
	}

	auto str = duckdb::StringVector::AddString(vec, field);
	duckdb::FlatVector::GetData<duckdb::string_t>(vec)[row_idx] = str;
}
void XMLBuffer::statementCallback(const RdfStatement &stmt) {

	// Safety check: If chunk is full, push to overflow and return
	if (_current_count >= STANDARD_VECTOR_SIZE) {
		RDFRow row;
		row.subject = stmt.subject;
		row.predicate = stmt.predicate;
		row.object = stmt.object;
		row.graph = "";
		row.datatype = stmt.datatype;
		row.lang = stmt.language;
		_overflow_buffer.push_back(std::move(row));
	}
	writeToVector(_current_chunk->data[0], _current_count, "");
	writeToVector(_current_chunk->data[1], _current_count, stmt.subject);
	writeToVector(_current_chunk->data[2], _current_count, stmt.predicate);
	writeToVector(_current_chunk->data[3], _current_count, stmt.object);
	writeToVector(_current_chunk->data[4], _current_count, stmt.datatype);
	writeToVector(_current_chunk->data[5], _current_count, stmt.language);
	_current_count++;
}

void XMLBuffer::namespaceCallback(std::string_view prefix, std::string_view uri) {
	std::cout << "[NS] " << (prefix.empty() ? "(default)" : prefix) << " => " << uri << "\n";
}
void XMLBuffer::errorCallback(const std::string &msg) {
	std::cerr << "![ERR] " << msg << std::endl;
}
