
#include "include/serd_buffer.hpp"
#include "duckdb/common/exception.hpp"
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
SerdBuffer::SerdBuffer(std::string path, std::string base_uri, const bool strict_parsing, const bool expand_prefixes)
    : _reader(nullptr, &serd_reader_free), _env(nullptr, &serd_env_free),
      ITriplesBuffer(path, base_uri, strict_parsing, expand_prefixes) {
	// Use "rb" instead of "rbe" - the "e" flag (O_CLOEXEC) is GNU-only and breaks Windows
	FILE *_f = std::fopen(_file_path.c_str(), "rb");
	if (!_f) {
		throw std::runtime_error("Could not open RDF file: " + _file_path);
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
	if (syntax != SERD_NQUADS && syntax != SERD_NTRIPLES) {
		_expand_prefixes = expand_prefixes;
	} else { // Prefixes don't make sense in triple/quad formats
		_expand_prefixes = false;
	}

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
	if (_reader.get())
		serd_reader_end_stream(_reader.get());
	serd_reader_free(_reader.release());
	if (_env.get())
		serd_env_free(_env.release());
	if (_file.get())
		std::fclose(_file.release());
}

/*
    Start parsing the RDF file. Only needs to be called once.
*/
void SerdBuffer::StartParse() {
	const char *fp;
	fp = _file_path.c_str();
	serd_reader_start_stream(_reader.get(), _file.get(), (uint8_t *)fp, false);
}

void SerdBuffer::WriteToVector(duckdb::Vector &vec, idx_t row_idx, const SerdNode *node) {
	if (!node || !node->buf) {
		duckdb::FlatVector::SetNull(vec, row_idx, true);
		return;
	}
	// Zero-copy from Serd buffer to DuckDB String Heap
	if (_expand_prefixes && node->type == SERD_CURIE) {
		SerdNode expanded = serd_env_expand_node(_env.get(), node);
		if (expanded.buf) {
			auto str = duckdb::StringVector::AddString(vec, (const char *)expanded.buf, expanded.n_bytes);
			duckdb::FlatVector::GetData<duckdb::string_t>(vec)[row_idx] = str;
			serd_node_free(&expanded);
			return;
		}
		// If expansion failed, fall through to adding the original CURIE
	}
	auto str = duckdb::StringVector::AddString(vec, (const char *)node->buf, node->n_bytes);
	duckdb::FlatVector::GetData<duckdb::string_t>(vec)[row_idx] = str;
}

void SerdBuffer::PopulateChunk(duckdb::DataChunk &output) {
	_current_chunk = &output;
	_current_count = 0;

	// 1. Drain overflow buffer first (if any)
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

	// 2. Parse from file directly into Chunk
	while (_current_count < STANDARD_VECTOR_SIZE && !_eof) {
		SerdStatus st = serd_reader_read_chunk(_reader.get());
		switch (st) {
		case SERD_SUCCESS:
			// Loop continues; Callback increments current_count
			_eof = false;
			break;

		case SERD_FAILURE:
			serd_reader_end_stream(_reader.get());
			if (std::feof(_file.get())) {
				_eof = true;
			} else {
				if (_has_error) {
					throw duckdb::SyntaxException(_error_message);
				} else {
					throw std::runtime_error("SERD failure");
				}
			}
			break;
		case SERD_ERR_BAD_CURIE:
		case SERD_ERR_ID_CLASH:
		case SERD_ERR_BAD_TEXT:
		case SERD_ERR_INTERNAL:
			throw std::runtime_error("SERD Error: " + SerdStatusToString(st));
		case SERD_ERR_BAD_SYNTAX:
			if (_strict_parsing) {
				if (_has_error) {
					throw duckdb::SyntaxException(_error_message);
				} else {
					throw duckdb::SyntaxException("SERD bad RDF syntax");
				}
			} else {
				if (serd_reader_skip_until_byte(_reader.get(), '\n') == SERD_FAILURE)
					throw std::runtime_error("SERD failure while skipping after syntax error");
			}
		default:
			throw std::runtime_error("SERD other error");
			break;
		}
	}

	output.SetCardinality(_current_count);
	_current_chunk = nullptr; // Clear pointer for safety
}

string SerdBuffer::SafeString(const SerdNode *node) {
	if (!node || !node->buf || node->n_bytes == 0)
		return {};
	std::string retVal;
	if (_expand_prefixes && node->type == SERD_CURIE) {
		SerdNode expanded = serd_env_expand_node(_env.get(), node);
		if (expanded.buf) {
			retVal = std::string(reinterpret_cast<const char *>(expanded.buf), expanded.n_bytes);
			serd_node_free(&expanded);
		} else {
			retVal = std::string(reinterpret_cast<const char *>(node->buf), node->n_bytes);
		}
	} else {
		retVal = std::string(reinterpret_cast<const char *>(node->buf), node->n_bytes);
	}
	return retVal;
}

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

	// Safety check: If chunk is full, push to overflow and return
	if (self->_current_count >= STANDARD_VECTOR_SIZE) {
		RDFRow row;
		row.subject = self->SafeString(subject);
		row.predicate = self->SafeString(predicate);
		row.object = self->SafeString(object);
		row.graph = self->SafeString(graph);
		row.datatype = self->SafeString(object_datatype);
		row.lang = self->SafeString(object_lang);
		self->_overflow_buffer.push_back(std::move(row));
		return SERD_SUCCESS;
	}

	// Fast Path: Direct Write to DuckDB Vectors
	// Note: DataChunk columns map to: 0:graph, 1:subject, 2:predicate, 3:object, ...
	self->WriteToVector(self->_current_chunk->data[0], self->_current_count, graph);
	self->WriteToVector(self->_current_chunk->data[1], self->_current_count, subject);
	self->WriteToVector(self->_current_chunk->data[2], self->_current_count, predicate);
	self->WriteToVector(self->_current_chunk->data[3], self->_current_count, object);
	self->WriteToVector(self->_current_chunk->data[4], self->_current_count, object_datatype);
	self->WriteToVector(self->_current_chunk->data[5], self->_current_count, object_lang);

	self->_current_count++;
	return SERD_SUCCESS;
}

// While SERD does provide a skip function for syntax errors (serd_reader_skip_until_byte)
// it doesn't seem like calling it actually helps.
SerdStatus SerdBuffer::ErrorCallBack(void *user_data, const SerdError *error) {
	auto *self = static_cast<SerdBuffer *>(user_data);
	if (self->_strict_parsing) {
		self->_has_error = true;
		self->_error_message =
		    "SERD parsing error '" + SerdStatusToString(error->status) + "', at line " + std::to_string(error->line);
		return SERD_FAILURE;
	} else
		return SERD_SUCCESS;
}

SerdStatus SerdBuffer::BaseCallback(void *user_data, const SerdNode *uri) {
	auto *self = static_cast<SerdBuffer *>(user_data);
	serd_env_set_base_uri(self->_env.get(), uri);
	return SERD_SUCCESS;
}

SerdStatus SerdBuffer::PrefixCallback(void *user_data, const SerdNode *name, const SerdNode *uri) {
	auto *self = static_cast<SerdBuffer *>(user_data);
	if (self->_expand_prefixes) {
		// Update SerdEnv with new prefix mapping
		serd_env_set_prefix(self->_env.get(), name, uri);
	}
	return SERD_SUCCESS;
}
