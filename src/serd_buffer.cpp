
#include "include/serd_buffer.hpp"
#include <iostream>
#include <stdexcept>

// Simple function to determine RDF syntax from file extension
static SerdSyntax SyntaxFromPath(const std::string &path) {
    auto pos = path.rfind('.');
    if (pos == std::string::npos) return SERD_NTRIPLES;
    std::string ext = path.substr(pos + 1);
    for (auto &c : ext) c = (char)tolower(c);
    if (ext == "ttl" || ext == "turtle") return SERD_TURTLE;
    if (ext == "nq" || ext == "nquads") return SERD_NQUADS;
    if (ext == "nt" || ext == "ntriples") return SERD_NTRIPLES;
    if (ext == "trig") return SERD_TRIG;
    // default fallback to N-Triples
    return SERD_NTRIPLES;
}

/*
    SerdBuffer constructor. Using managed pointers for the SERD calls
*/
SerdBuffer::SerdBuffer(const std::string &path, const std::string &base_uri)
    :   _file(nullptr, &fclose),
        _reader(nullptr, &serd_reader_free),
        _env(nullptr,&serd_env_free) {
    file_path = path;
    FILE* _f = std::fopen(file_path.c_str(), "rbe");
    if (!_f) {
        throw std::runtime_error("Could not open RDF file: "+file_path);
    }
    _file.reset(_f);
    SerdNode base = SERD_NODE_NULL;
    if(!base_uri.empty()){
        base = serd_node_from_string(SERD_URI, (const uint8_t*)base_uri.c_str());
    }
    SerdEnv* t_env  = serd_env_new(&base);
    if (!t_env) {
        throw std::runtime_error("Unable to create serd environment");
    }
    _env.reset(t_env);
    SerdSyntax syntax = SyntaxFromPath(path);
    SerdReader* t_reader = serd_reader_new(syntax, this, nullptr, &BaseCallback, &PrefixCallback, &StatementCallback, nullptr);
    if (!t_reader) {
        throw std::runtime_error("serd_reader_new failed");
    }
    serd_reader_set_error_sink(t_reader, &ErrorCallBack, this);
    _reader.reset(t_reader);
}
    
SerdBuffer::~SerdBuffer() = default;

/*
    Start parsing the RDF file. Only needs to be called once.
*/
void SerdBuffer::StartParse(){
    const char* fp;
    fp =file_path.c_str();
    serd_reader_start_stream(_reader.get(),_file.get(),(uint8_t*) fp,false);
}
    
/*
    Returns true if all RDF has been parsed
    AND rows have been processed.
    Handles invoking SERD to refresh the buffer as needed.
*/
bool SerdBuffer::EverythingProcessed(){
    if(rows.empty()){
       // cerr << "Rows empty\n";
        if(eof){
         //   cerr << "EOF reached\n";
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
RDFRow SerdBuffer::GetNextRow(){
    if(rows.empty()){
        throw std::runtime_error("Invoked after end of file");
    }
    RDFRow r = rows.front();
    rows.pop();
    return r;
}

void SerdBuffer::ParseNextBatch(uint64_t min_rows){
    while (rows.size() < min_rows && !eof) { 
        SerdStatus st = serd_reader_read_chunk(_reader.get());
//cerr << "Read a chunk, status: " << st << "\n";
        switch(st){
            case SERD_SUCCESS:
                eof = false;
                break;
            case SERD_FAILURE:
                serd_reader_end_stream(_reader.get());
                if(std::feof(_file.get())){
              //      cerr << "End of file reached, closing stream\n";   
                    eof = true;
                } else {
                    throw std::runtime_error("SERD failure");
                }
                break;
            case SERD_ERR_BAD_CURIE:
                throw std::runtime_error("SERD bad CURIE error");
            case SERD_ERR_ID_CLASH:
                throw std::runtime_error("SERD ID clash error");    
            case SERD_ERR_BAD_TEXT:
                throw std::runtime_error("SERD bad text encoding");
            case SERD_ERR_BAD_SYNTAX:
                throw std::runtime_error("SERD bad RDF syntax");
            case SERD_ERR_INTERNAL:
                throw std::runtime_error("SERD internal error");
            default:
                throw std::runtime_error("SERD other error");
                break;
        }
    }
}

auto safe_str = [](const SerdNode* node) -> std::string {
    if (!node || !node->buf || node->n_bytes == 0) return {};
    return std::string(reinterpret_cast<const char*>(node->buf), node->n_bytes);
};

SerdStatus SerdBuffer::StatementCallback(void *user_data,
    SerdStatementFlags ,const SerdNode *graph,
    const SerdNode *subject, const SerdNode *predicate,
    const SerdNode *object, const SerdNode *object_datatype,
    const SerdNode *object_lang){
      //  cerr << ".";
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

SerdStatus SerdBuffer::ErrorCallBack(void *user_data, const SerdError *error){
    auto *self = static_cast<SerdBuffer *>(user_data);
    throw std::runtime_error("SERD parsing error "+std::to_string(error->status)+ ", at line " + std::to_string(error->line));
    return SERD_SUCCESS;
}
SerdStatus SerdBuffer::BaseCallback(void *, const SerdNode *) { return SERD_SUCCESS; }
SerdStatus SerdBuffer::PrefixCallback(void *, const SerdNode *, const SerdNode *) { return SERD_SUCCESS; }