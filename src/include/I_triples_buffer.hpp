
#ifndef I_TRIPLES_BUFFER_H
#define I_TRIPLES_BUFFER_H
#include "duckdb.hpp"
#include <queue>

/*
    Holder for a single row of RDF
*/
struct RDFRow {
	std::string graph;
	std::string subject;
	std::string predicate;
	std::string object;
	std::string datatype;
	std::string lang;
};

class ITriplesBuffer {
public:
	// Supported file type hints for parsing
	enum FileType { TURTLE = 0, NQUADS, NTRIPLES, TRIG, XML, UNKNOWN };

	ITriplesBuffer(std::string path, std::string base_uri, bool strict_parsing = true,
	               const bool expand_prefixes = false)
	    : _file(nullptr, &fclose), _base_uri(std::move(base_uri)), _file_path(std::move(path)) {};

	virtual void PopulateChunk(duckdb::DataChunk &output) = 0;
	virtual void StartParse() = 0;
	virtual ~ITriplesBuffer() = default;

protected:
	std::unique_ptr<FILE, decltype(&fclose)> _file;
	std::string _base_uri;
	std::string _file_path;

	duckdb::DataChunk *_current_chunk = nullptr;
	duckdb::idx_t _current_count = 0;
	std::deque<RDFRow> _overflow_buffer;
	bool _eof = false;
	bool _strict_parsing = true;
	bool _expand_prefixes = false;
};

#endif // I_TRIPLES_BUFFER_H
