#ifndef SERD_BUFFER_H
#define SERD_BUFFER_H

#include <string>
#include <queue>
#include <serd/serd.h>
#include <memory>
using namespace std;

/*
    Holder for a single row of RDP
*/
struct RDFRow {
	string graph;
	string subject;
	string predicate;
	string object;
	string datatype;
	string lang;
};

/*
    Buffer that reads RDF data from a file using Serd and stores it in memory
*/
class SerdBuffer {
public:
	SerdBuffer(const std::string &path, const std::string &base_uri, const bool strict_parsing = true);

	~SerdBuffer();

	void StartParse();
	RDFRow GetNextRow();
	bool EverythingProcessed();

private:
	void ParseNextBatch(uint64_t min_rows);
	static string SerdStatusToString(SerdStatus status);
	static SerdStatus StatementCallback(void *user_data, SerdStatementFlags /*flags*/, const SerdNode *graph,
	                                    const SerdNode *subject, const SerdNode *predicate, const SerdNode *object,
	                                    const SerdNode *object_datatype, const SerdNode *object_lang);
	static SerdStatus ErrorCallBack(void *user_data, const SerdError *error);
	static SerdStatus BaseCallback(void *, const SerdNode *);
	static SerdStatus PrefixCallback(void *, const SerdNode *, const SerdNode *);

private:
	std::unique_ptr<SerdReader, decltype(&serd_reader_free)> _reader;
	std::unique_ptr<FILE, decltype(&fclose)> _file;
	std::unique_ptr<SerdEnv, decltype(&serd_env_free)> _env;
	std::string file_path;
	std::queue<RDFRow> rows;
	bool eof = false;
	bool _strict_parsing = true;
	uint64_t target_rows;
};

#endif // SERD_BUFFER_H
