#ifndef XML_BUFFER_H
#define XML_BUFFER_H

#include "I_triples_buffer.hpp"
#include "duckdb.hpp"
#include <string_view>
#include "rdf_xml_parser.hpp"

class XMLBuffer : public ITriplesBuffer {
public:
	XMLBuffer(std::string path, std::string base_uri, const bool strict_parsing = true,
	          const bool expand_prefixes = false);

	~XMLBuffer();

	void PopulateChunk(duckdb::DataChunk &output);
	void StartParse();

private:
	void writeToVector(duckdb::Vector &vec, idx_t row_idx, const std::string &field);
	void statementCallback(const RdfStatement &stmt);
	void namespaceCallback(std::string_view prefix, std::string_view uri);
	void errorCallback(const std::string &msg);
	RdfXmlParser _parser;
};

#endif // XML_BUFFER_H
