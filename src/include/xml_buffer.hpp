#ifndef XML_BUFFER_H
#define XML_BUFFER_H

#include "I_triples_buffer.hpp"
#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "rdf_xml_parser.hpp"

class XMLBuffer : public ITriplesBuffer {
public:
	XMLBuffer(std::string path, std::string base_uri, duckdb::FileSystem *fs = nullptr,
			  const bool strict_parsing = true, const bool expand_prefixes = false,
			  ITriplesBuffer::FileType file_type = ITriplesBuffer::UNKNOWN);

	~XMLBuffer();

	void PopulateChunk(duckdb::DataChunk &output);
	void StartParse();

private:
	constexpr static size_t PARSING_CHUNK_SIZE = 4096;
	void writeToVector(duckdb::Vector &vec, idx_t row_idx, const std::string &field);
	void statementCallback(const RdfStatement &stmt);
	void namespaceCallback(const std::string &prefix, const std::string &uri);
	void errorCallback(const std::string &msg);
	RdfXmlParser _parser;
};

#endif // XML_BUFFER_H
