#ifndef SERD_BUFFER_H
#define SERD_BUFFER_H

#include <string>
#include "duckdb.hpp"
#include <serd/serd.h>
#include "I_triples_buffer.hpp"
#include <memory>
using namespace std;

/*
    Buffer that reads RDF data from a file using Serd and stores it in memory
*/
class SerdBuffer : public ITriplesBuffer {
public:
	SerdBuffer(std::string path, std::string base_uri, const bool strict_parsing = true,
	           const bool expand_prefixes = false, ITriplesBuffer::FileType file_type = ITriplesBuffer::UNKNOWN);

	~SerdBuffer();

	void PopulateChunk(duckdb::DataChunk &output);
	void StartParse();

private:
	// Helper to write to vector
	void WriteToVector(duckdb::Vector &vec, idx_t row_idx, const SerdNode *node);
	string SafeString(const SerdNode *node);
	static string SerdStatusToString(SerdStatus status);
	static SerdStatus StatementCallback(void *user_data, SerdStatementFlags /*flags*/, const SerdNode *graph,
	                                    const SerdNode *subject, const SerdNode *predicate, const SerdNode *object,
	                                    const SerdNode *object_datatype, const SerdNode *object_lang);
	static SerdStatus ErrorCallBack(void *user_data, const SerdError *error);
	static SerdStatus BaseCallback(void *, const SerdNode *);
	static SerdStatus PrefixCallback(void *, const SerdNode *, const SerdNode *);

private:
	std::unique_ptr<SerdReader, decltype(&serd_reader_free)> _reader;
	std::unique_ptr<SerdEnv, decltype(&serd_env_free)> _env;

	bool _has_error = false;
	std::string _error_message;
	uint64_t target_rows;
};

#endif // SERD_BUFFER_H
