#ifndef RDF_XML_PARSER_H
#define RDF_XML_PARSER_H

#include <iostream>
#include <string>
#include <functional>
#include <vector>
#include <map>
#include <cstring>
#include <libxml/parser.h>
#include <libxml/SAX2.h>
#include <memory>

struct RdfStatement {
	std::string subject;
	std::string predicate;
	std::string object;
	std::string datatype;
	std::string language;
};

class RdfXmlParser {
public:
	using StatementCallback = std::function<void(const RdfStatement &)>;
	using NamespaceCallback = std::function<void(const std::string &prefix, const std::string &uri)>;
	using ErrorCallback = std::function<void(const std::string &message)>;

	RdfXmlParser(StatementCallback s_cb = nullptr, NamespaceCallback n_cb = nullptr, ErrorCallback e_cb = nullptr,
	             std::string base = "");
	// must be called with is_final true on the last chunk otherwise libxml may
	// leak resources
	void parseChunk(const char *chunk, int size, bool is_final);
	~RdfXmlParser();
	void addNameSpace(const std::string &prefix, const std::string &uri);
	void setBlankNodePrefix(const std::string &prefix) {
		_blank_node_prefix = prefix;
	}

private:
	const std::string RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	const std::string XML_NS = "http://www.w3.org/XML/1998/namespace";
	constexpr static char const *LANG_TAG = "lang";
	constexpr static char const *BASE_TAG = "base";
	constexpr static char const *ABOUT_ATTR = "about";
	constexpr static char const *ID_ATTR = "ID";
	constexpr static char const *NODE_ID_ATTR = "nodeID";
	constexpr static char const *RESOURCE_ATTR = "resource";
	constexpr static char const *DATATYPE_ATTR = "datatype";
	constexpr static char const *PARSE_TYPE_ATTR = "parseType";
	StatementCallback on_statement;
	NamespaceCallback on_namespace;
	ErrorCallback on_error;
	std::string base_uri;
	bool _at_eof = false;
	unsigned long bnode_count;
	std::string _blank_node_prefix = "_:b";
	std::unique_ptr<xmlParserCtxt, decltype(&xmlFreeParserCtxt)> _ctxt;
	std::map<std::string, std::string> _nameSpaces;
	enum class ElementType { NODE, PROPERTY, ROOT };

	struct ElementFrame {
		ElementType type;
		std::string uri;
		std::string lang;
		std::string datatype;
		std::string reify_id; // For rdf:ID on properties (Reification)
		std::string text_buf;
		std::string baseURI; // Current base URI
		bool has_obj_nodes = false;
		bool is_xml_literal = false; // For rdf:parseType="Literal"
		int literal_depth = 0;       // For tracking nested XML in XMLLiteral
		ElementFrame(ElementType t, std::string u, std::string l, std::string d, std::string r, std::string tb,
		             std::string bu, bool obj, bool xml)
		    : type(t), uri(u), lang(l), datatype(d), reify_id(r), text_buf(tb), baseURI(bu), has_obj_nodes(obj),
		      is_xml_literal(xml) {
		}
	};

	std::vector<ElementFrame> _stack;

	xmlSAXHandler saxHandler;

	static bool isAbsolute(const std::string &uri);
	std::string generateBNode();
	static std::string xmlEscape(const std::string &data);
	static std::string xmlEscape(const char *data, int len);
	std::string literalXML(const xmlChar *localname, const xmlChar *prefix, const xmlChar **namespaces,
	                       int nb_namespaces, const xmlChar **attributes, int nb_attributes);
	std::string currentBaseURI();
	bool isReservedAttr(const std::string &uri);
	void setupSAX();
	void processAttributes(int nb_attributes, const xmlChar **attributes, const std::string &subject,
	                       const std::string &lang);
	static void onStartElement(void *ctx, const xmlChar *localname, const xmlChar *prefix, const xmlChar *URI,
	                           int nb_namespaces, const xmlChar **namespaces, int nb_attributes, int nb_defaulted,
	                           const xmlChar **attributes);

	static void onEndElement(void *ctx, const xmlChar *localname, const xmlChar *prefix, const xmlChar *URI);
	static void onCharacters(void *ctx, const xmlChar *ch, int len);

	void emitWithReification(const std::string &s, const std::string &p, const std::string &o, const std::string &dt,
	                         const std::string &lang, const std::string &r_id);
	void emit(const std::string &s, const std::string &p, const std::string &o, const std::string &dt,
	          const std::string &lang);

	std::string findAttr(int nb_attributes, const xmlChar **attributes, const std::string &ns,
	                     const std::string &local);

	std::string expandUri(const xmlChar *URI, const xmlChar *localname);

	std::string trim(const std::string &s);
};

#endif // RDF_XML_PARSER_H
