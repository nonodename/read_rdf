#ifndef RDF_XML_PARSER_H
#define RDF_XML_PARSER_H

#include <iostream>
#include <string>
#include <string_view>
#include <functional>
#include <stack>
#include <cstring>
#include <libxml/parser.h>
#include <libxml/SAX2.h>

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
	using NamespaceCallback = std::function<void(std::string_view prefix, std::string_view uri)>;
	using ErrorCallback = std::function<void(const std::string &message)>;

	RdfXmlParser(StatementCallback s_cb = nullptr, NamespaceCallback n_cb = nullptr, ErrorCallback e_cb = nullptr,
	             std::string base = "");

	void parseChunk(const char *chunk, int size, bool is_final);
	~RdfXmlParser();

private:
	StatementCallback on_statement;
	NamespaceCallback on_namespace;
	ErrorCallback on_error;
	std::string base_uri;
	uint64_t bnode_count;
	std::unique_ptr<xmlParserCtxt, decltype(&xmlFreeParserCtxt)> _ctxt;

	enum class ElementType { NODE, PROPERTY, ROOT };

	struct ElementFrame {
		ElementType type;
		std::string uri;
		std::string lang;
		std::string datatype;
		std::string reify_id; // For rdf:ID on properties (Reification)
		std::string text_buf;
		bool has_obj_nodes = false;
		bool is_xml_literal = false; // For rdf:parseType="Literal"
		ElementFrame(ElementType t, std::string u, std::string l, std::string d, std::string r, std::string tb,
		             bool obj, bool xml)
		    : type(t), uri(u), lang(l), datatype(d), reify_id(r), text_buf(tb), has_obj_nodes(obj),
		      is_xml_literal(xml) {
		}
	};

	std::stack<ElementFrame> stack;
	const std::string RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	const std::string XML_NS = "http://www.w3.org/XML/1998/namespace";

	xmlSAXHandler saxHandler;

	std::string generateBNode();

	bool isReservedAttr(const std::string &uri);
	void setupSAX();

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
