
#include "include/serd_buffer.hpp"
#include <iostream>
#include <stdexcept>
#include <memory>

// Simple function to determine RDF syntax from file extension
static SerdSyntax SyntaxFromPath(const std::string &path) {
	auto pos = path.rfind('.');
	if (pos == std::string::npos)
		return SERD_NTRIPLES;
	std::string ext = path.substr(pos + 1);
	for (auto &c : ext)
		c = (char)tolower(c);
	if (ext == "ttl" || ext == "turtle")
		return SERD_TURTLE;
	if (ext == "nq" || ext == "nquads")
		return SERD_NQUADS;
	if (ext == "nt" || ext == "ntriples")
		return SERD_NTRIPLES;
	if (ext == "trig")
		return SERD_TRIG;
	// default fallback to N-Triples
	return SERD_NTRIPLES;
}

/*
    SerdBuffer constructor. Using managed pointers for the SERD calls
*/
SerdBuffer::SerdBuffer(const std::string &path, const std::string &base_uri, const bool strict_parsing)
    : _file(nullptr, &fclose), _reader(nullptr, &serd_reader_free), _env(nullptr, &serd_env_free) {
	file_path = path;
	// Use "rb" instead of "rbe" - the "e" flag (O_CLOEXEC) is GNU-only and breaks Windows
	FILE *_f = std::fopen(file_path.c_str(), "rb");
	if (!_f) {
		throw std::runtime_error("Could not open RDF file: " + file_path);
	}
	_file.reset(_f);
	SerdNode base = SERD_NODE_NULL;
	if (!base_uri.empty()) {
		base = serd_node_from_string(SERD_URI, (const uint8_t *)base_uri.c_str());
	}
	SerdEnv *t_env = serd_env_new(&base);
	if (!t_env) {
		throw std::runtime_error("Unable to create serd environment");
	}
	_env.reset(t_env);
	_strict_parsing = strict_parsing;
	SerdSyntax syntax = SyntaxFromPath(path);
	SerdReader *t_reader =
	    serd_reader_new(syntax, this, nullptr, &BaseCallback, &PrefixCallback, &StatementCallback, nullptr);
	if (!t_reader) {
		throw std::runtime_error("serd_reader_new failed");
	}
	serd_reader_set_strict(t_reader, strict_parsing);
	serd_reader_set_error_sink(t_reader, &ErrorCallBack, this);
	_reader.reset(t_reader);
}

SerdBuffer::~SerdBuffer() {
	if(_reader.get())
		serd_reader_end_stream(_reader.get());
	serd_reader_free(_reader.release());
	if(_env.get())
		serd_env_free(_env.release());
	if(_file.get())
		std::fclose(_file.release());
}

/*
    Start parsing the RDF file. Only needs to be called once.
*/
void SerdBuffer::StartParse() {
	const char *fp;
	fp = file_path.c_str();
	serd_reader_start_stream(_reader.get(), _file.get(), (uint8_t *)fp, false);
}

/*
    Returns true if all RDF has been parsed
    AND rows have been processed.
    Handles invoking SERD to refresh the buffer as needed.
*/
bool SerdBuffer::EverythingProcessed() {
	if (rows.empty()) {
		if (eof) {
			return true;
		}
		ParseNextBatch(10);
		return rows.empty();
	} else {
		return false;
	}
}
/*
    Get the next RDF row from the buffer.
    Throws if invoked after EverythingProcessed() returns true.
*/
RDFRow SerdBuffer::GetNextRow() {
	if (rows.empty()) {
		throw std::runtime_error("Invoked after end of file");
	}
	RDFRow r = rows.front();
	rows.pop();
	return r;
}

void SerdBuffer::ParseNextBatch(uint64_t min_rows) {
	while (rows.size() < min_rows && !eof) {
		SerdStatus st = serd_reader_read_chunk(_reader.get());
		switch (st) {
		case SERD_SUCCESS:
			eof = false;
			break;
		case SERD_FAILURE:
			serd_reader_end_stream(_reader.get());
			if (std::feof(_file.get())) {
				eof = true;
			} else {
				throw std::runtime_error("SERD failure");
			}
			break;
		case SERD_ERR_BAD_CURIE:
		case SERD_ERR_ID_CLASH:
		case SERD_ERR_BAD_TEXT:
		case SERD_ERR_INTERNAL:
			throw std::runtime_error("SERD Error: " + SerdStatusToString(st));
		case SERD_ERR_BAD_SYNTAX:
			if (_strict_parsing)
				throw std::runtime_error("SERD bad RDF syntax");
			else {
				cerr << "Skipping in parse next batch";
				if (serd_reader_skip_until_byte(_reader.get(), '\n') == SERD_FAILURE)
					throw std::runtime_error("SERD failure while skipping after syntax error");
			}
		default:
			throw std::runtime_error("SERD other error");
			break;
		}
	}
}

auto safe_str = [](const SerdNode *node) -> std::string {
	if (!node || !node->buf || node->n_bytes == 0)
		return {};
	return std::string(reinterpret_cast<const char *>(node->buf), node->n_bytes);
};

string SerdBuffer::SerdStatusToString(SerdStatus status) {
	switch (status) {
	case SERD_SUCCESS:
		return "Success";
	case SERD_FAILURE:
		return "Non-fatal failure";
	case SERD_ERR_UNKNOWN:
		return "Unknown error";
	case SERD_ERR_BAD_SYNTAX:
		return "Invalid syntax";
	case SERD_ERR_BAD_ARG:
		return "Invalid argument";
	case SERD_ERR_NOT_FOUND:
		return "Not found";
	case SERD_ERR_ID_CLASH:
		return "ID clash";
	case SERD_ERR_BAD_CURIE:
		return "Bad CURIE";
	case SERD_ERR_INTERNAL:
		return "Internal error";
	case SERD_ERR_BAD_WRITE:
		return "Write error";
	case SERD_ERR_BAD_TEXT:
		return "Bad text encoding";
	default:
		return "Unrecognized SerdStatus";
	}
}

SerdStatus SerdBuffer::StatementCallback(void *user_data, SerdStatementFlags, const SerdNode *graph,
                                         const SerdNode *subject, const SerdNode *predicate, const SerdNode *object,
                                         const SerdNode *object_datatype, const SerdNode *object_lang) {
	auto *self = static_cast<SerdBuffer *>(user_data);
	RDFRow row;
	row.graph = safe_str(graph);
	row.subject = safe_str(subject);
	row.predicate = safe_str(predicate);
	row.object = safe_str(object);
	row.datatype = safe_str(object_datatype);
	row.lang = safe_str(object_lang);
	self->rows.push(std::move(row));
	return SERD_SUCCESS;
}

// While SERD does provide a skip function for syntax errors (serd_reader_skip_until_byte)
// it doesn't seem like calling it actually helps.
SerdStatus SerdBuffer::ErrorCallBack(void *user_data, const SerdError *error) {
	auto *self = static_cast<SerdBuffer *>(user_data);
	if (self->_strict_parsing)
		throw std::runtime_error("SERD parsing error '" + SerdStatusToString(error->status) + "', at line " +
		                         std::to_string(error->line));
	return SERD_SUCCESS;
}
SerdStatus SerdBuffer::BaseCallback(void *, const SerdNode *) {
	return SERD_SUCCESS;
}
SerdStatus SerdBuffer::PrefixCallback(void *, const SerdNode *, const SerdNode *) {
	return SERD_SUCCESS;
}
