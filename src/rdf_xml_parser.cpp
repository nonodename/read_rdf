#include "include/rdf_xml_parser.hpp"
#include <libxml/uri.h>
#include <sstream>
#include <libxml/entities.h>

struct XmlFreeDeleter {
    void operator()(xmlChar* ptr) const {
        if (ptr) {
            xmlFree(ptr);
        }
    }
};

struct XmlFreeURIDeleter {
    void operator()(xmlURI* ptr) const {
        if (ptr) {
            xmlFreeURI(ptr);
        }
    }
};

using XmlCharPtr = std::unique_ptr<xmlChar, XmlFreeDeleter>;
using xmlURISmartPtr = std::unique_ptr<xmlURI, XmlFreeURIDeleter>;

RdfXmlParser::RdfXmlParser(StatementCallback s_cb, NamespaceCallback n_cb, ErrorCallback e_cb, std::string base)
    : on_statement(s_cb), on_namespace(n_cb), on_error(e_cb), base_uri(base), bnode_count(0),
      _ctxt(nullptr, &xmlFreeParserCtxt) {
	setupSAX();

	xmlParserCtxtPtr ctxt = xmlCreatePushParserCtxt(&saxHandler, this, nullptr, 0, nullptr);
	_ctxt.reset(ctxt);
}

void RdfXmlParser::parseChunk(const char *chunk, int size, bool is_final) {
	if (_at_eof) {
		return;
	}
	_at_eof = is_final;
	if (!_ctxt)
		throw std::runtime_error("Failed to create XML parser context");
	if (xmlParseChunk(_ctxt.get(), chunk, size, is_final) != 0) {
		on_error("XML parsing error");
	}
}

void RdfXmlParser::addNameSpace(const std::string &prefix, const std::string &uri) {
	_nameSpaces[prefix] = uri;
}

RdfXmlParser::~RdfXmlParser() {
}

bool RdfXmlParser::isAbsolute(const std::string &uri) {
	xmlURISmartPtr uri_parsed (xmlParseURI(uri.c_str()));
	bool has_scheme = (uri_parsed != NULL && uri_parsed->scheme != NULL);
	return has_scheme;
}

std::string RdfXmlParser::generateBNode() {
	return _blank_node_prefix + std::to_string(++bnode_count);
}

bool RdfXmlParser::isReservedAttr(const std::string &uri) {
	if (uri.find(XML_NS) == 0)
		return true;
	if (uri.find(RDF_NS) != 0)
		return false;
	std::string local = uri.substr(RDF_NS.length());
	return (local == ABOUT_ATTR || local == ID_ATTR || local == NODE_ID_ATTR || local == RESOURCE_ATTR ||
	        local == DATATYPE_ATTR || local == PARSE_TYPE_ATTR);
}

void RdfXmlParser::setupSAX() {
	memset(&saxHandler, 0, sizeof(saxHandler));
	saxHandler.initialized = XML_SAX2_MAGIC;
	saxHandler.startElementNs = &RdfXmlParser::onStartElement;
	saxHandler.endElementNs = &RdfXmlParser::onEndElement;
	saxHandler.characters = &RdfXmlParser::onCharacters;
}

std::string RdfXmlParser::currentBaseURI() {
	if (_stack.empty())
		return base_uri;
	std::string result = "";
	for (const auto &element : _stack) {
		if (!element.baseURI.empty()) {
			result = element.baseURI;
		}
	}
	return result;
}

std::string RdfXmlParser::xmlEscape(const std::string &data) {
	const xmlChar *input = reinterpret_cast<xmlChar *>(const_cast<char *>(data.c_str()));
	XmlCharPtr escaped (xmlEncodeSpecialChars(nullptr, input));
	std::string res = std::string(reinterpret_cast<const char *>(escaped.get()));
	return res;
}

std::string RdfXmlParser::xmlEscape(const char *data, int len) {
	char *temp = new char[len + 1];
	memcpy(temp, data, len);
	temp[len] = '\0';
	const xmlChar *input = reinterpret_cast<const xmlChar *>(temp);
	XmlCharPtr escaped (xmlEncodeSpecialChars(nullptr, input));
	std::string res = std::string(reinterpret_cast<const char *>(escaped.get()));
	delete[] temp;
	return res;
}

std::string RdfXmlParser::literalXML(const xmlChar *localname, const xmlChar *prefix, const xmlChar **namespaces,
                                     int nb_namespaces, const xmlChar **attributes, int nb_attributes) {
	std::ostringstream oss;
	oss << "<";
	if (prefix)
		oss << (const char *)prefix << ":";
	oss << (const char *)localname;

	for (int i = 0; i < nb_namespaces; ++i) {
		oss << " xmlns";
		if (namespaces[i * 2])
			oss << ":" << (const char *)namespaces[i * 2];
		oss << "=\"" << (const char *)namespaces[i * 2 + 1] << "\"";
	}

	for (int i = 0; i < nb_attributes; ++i) {
		oss << " " << (const char *)attributes[i * 5];
		std::string val((const char *)attributes[i * 5 + 3],
		                (const char *)attributes[i * 5 + 4] - (const char *)attributes[i * 5 + 3]);
		oss << "=\"" << xmlEscape(val) << "\"";
	}
	oss << ">";
	return oss.str();
}

void RdfXmlParser::processAttributes(int nb_attributes, const xmlChar **attributes, const std::string &subject,
                                     const std::string &lang) {
	for (int i = 0; i < nb_attributes; ++i) {
		auto attr_uri = expandUri(attributes[i * 5 + 2], attributes[i * 5]);
		if (!isReservedAttr(attr_uri)) {
			std::string val((const char *)attributes[i * 5 + 3],
			                (const char *)attributes[i * 5 + 4] - (const char *)attributes[i * 5 + 3]);
			emit(subject, attr_uri, val, "", lang);
		}
	}
}

RdfAttributes RdfXmlParser::parseAttributes(int nb_attributes, const xmlChar **attributes) {
	RdfAttributes result;
	for (int i = 0; i < nb_attributes; ++i) {
		const xmlChar* localname = attributes[i * 5];
		const xmlChar* value_start = attributes[i * 5 + 3];
		const xmlChar* value_end = attributes[i * 5 + 4];
		if (xmlStrEqual(localname, (const xmlChar*)ABOUT_ATTR)) {
			result.about = LibXMLView(value_start, value_end);
		} else if (xmlStrEqual(localname, (const xmlChar*)ID_ATTR)) {
			result.rdf_id = LibXMLView(value_start, value_end);
		} else if (xmlStrEqual(localname, (const xmlChar*)NODE_ID_ATTR)) {
			result.nodeID = LibXMLView(value_start, value_end);
		} else if (xmlStrEqual(localname, (const xmlChar*)RESOURCE_ATTR)) {
			result.resource = LibXMLView(value_start, value_end);
		} else if (xmlStrEqual(localname, (const xmlChar*)DATATYPE_ATTR)) {
			result.datatype = LibXMLView(value_start, value_end);
		} else if (xmlStrEqual(localname, (const xmlChar*)PARSE_TYPE_ATTR)) {
			result.parseType = LibXMLView(value_start, value_end);
		} else if (xmlStrEqual(localname, (const xmlChar*)LANG_TAG)) {
			result.lang = LibXMLView(value_start, value_end);
		} else if (xmlStrEqual(localname, (const xmlChar*)BASE_TAG)) {
			result.base = LibXMLView(value_start, value_end);
		}
	}
	return result;
}

// idea here is to create a helper function that will iterate through the attributes
// once and return a struct of named attributes we care about
void RdfXmlParser::onStartElement(void *ctx, const xmlChar *localname, const xmlChar *prefix, const xmlChar *URI,
                                  int nb_namespaces, const xmlChar **namespaces, int nb_attributes, int nb_defaulted,
                                  const xmlChar **attributes) {
	auto *self = static_cast<RdfXmlParser *>(ctx);
	ElementFrame *parent_frame = self->_stack.empty() ? nullptr : &(self->_stack.back());

	// Handle Nested XMLLiteral
	if (parent_frame && parent_frame->type == ElementType::PROPERTY_XML_LITERAL) {
		parent_frame->literal_depth++;
		parent_frame->text_buf +=
		    self->literalXML(localname, prefix, namespaces, nb_namespaces, attributes, nb_attributes);
		return;
	}

	for (int i = 0; i < nb_namespaces; ++i) {
		std::string p_str = namespaces[i * 2] ? (const char *)namespaces[i * 2] : "";
		self->on_namespace(p_str, (const char *)namespaces[i * 2 + 1]);
	}

	std::string current_uri = self->expandUri(URI, localname);

	// Handle rdf:li container membership
	if (current_uri == self->RDF_NS + "li" && parent_frame && parent_frame->type == ElementType::NODE) {
		parent_frame->li_counter++;
		current_uri = self->RDF_NS + "_" + std::to_string(parent_frame->li_counter);
	}
	auto attrs = self->parseAttributes(nb_attributes, attributes);
	
	if (current_uri == self->RDF_NS + "RDF") {
		self->_stack.emplace_back(ElementType::ROOT, "", attrs.lang.toString(), "", "", "",
		                          attrs.base.toString(), false);
		return;
	}

	ElementType parent_type = parent_frame ? parent_frame->type : ElementType::ROOT;
	bool is_node = (parent_type == ElementType::PROPERTY || parent_type == ElementType::PROPERTY_COLLECTION ||
	                parent_type == ElementType::ROOT);
	ElementType current_type = is_node ? ElementType::NODE : ElementType::PROPERTY;

	
	auto about = attrs.about.toString();
	auto nodeID = attrs.nodeID.toString();
	auto rdf_id = attrs.rdf_id.toString();
	auto resource = attrs.resource.toString();
	auto datatype = attrs.datatype.toString();
	auto parseType = attrs.parseType.toString();
	auto lang = attrs.lang.toString();
	auto base = attrs.base.toString();

	if (lang.empty() && parent_frame)
		lang = parent_frame->lang;
	if (base.empty())
		base = parent_frame ? parent_frame->baseURI : self->base_uri;

	if (current_type == ElementType::NODE) {
		std::string subject;
		if (!about.empty())
			subject = about;
		else if (!rdf_id.empty())
			subject = self->currentBaseURI() + "#" + rdf_id;
		else if (!nodeID.empty())
			subject = "_:" + nodeID;
		else
			subject = self->generateBNode();

		if (parent_frame && (parent_type == ElementType::PROPERTY || parent_type == ElementType::PROPERTY_COLLECTION)) {
			if (parent_type == ElementType::PROPERTY_COLLECTION) {
				std::string list_node = self->generateBNode();
				if (parent_frame->collection_tail.empty()) {
					std::string prop_subject = (self->_stack.end() - 2)->uri;
					self->emit(prop_subject, parent_frame->uri, list_node, "", "");
				} else {
					self->emit(parent_frame->collection_tail, self->REST_URI, list_node, "", "");
				}
				parent_frame->collection_tail = list_node;
				self->emit(list_node, self->FIRST_URI, subject, "", "");
			} else {
				parent_frame->has_obj_nodes = true;
				std::string prop_subject = (self->_stack.end() - 2)->uri;
				self->emitWithReification(prop_subject, parent_frame->uri, subject, "", "", parent_frame->reify_id);
			}
		}

		if (current_uri != self->RDF_NS + "Description")
			self->emit(subject, self->RDF_NS + "type", current_uri, "", "");

		self->processAttributes(nb_attributes, attributes, subject, lang);
		self->_stack.emplace_back(ElementType::NODE, subject, lang, "", "", "", base, false);
	} else { // PROPERTY
		auto reify_uri = rdf_id.empty() ? "" : self->currentBaseURI() + "#" + rdf_id;

		if (parseType == "Literal") {
			self->_stack.emplace_back(ElementType::PROPERTY_XML_LITERAL, current_uri, lang, datatype, reify_uri, "", "",
			                          false);
		} else if (parseType == "Collection") {
			self->_stack.emplace_back(ElementType::PROPERTY_COLLECTION, current_uri, lang, datatype, reify_uri, "", "",
			                          false);
		} else if (parseType == "Resource") {
			auto bnode = self->generateBNode();
			self->emitWithReification(parent_frame->uri, current_uri, bnode, "", "", reify_uri);
			self->_stack.emplace_back(ElementType::NODE, bnode, lang, "", "", "", "", false);
		} else if (!resource.empty() || !nodeID.empty()) { // Empty property element
			auto object = !resource.empty() ? resource : "_:" + nodeID;
			if (!resource.empty() && !isAbsolute(object))
				object = self->currentBaseURI() + object;
			self->emitWithReification(parent_frame->uri, current_uri, object, "", "", reify_uri);
			self->processAttributes(nb_attributes, attributes, object, lang);
			self->_stack.emplace_back(ElementType::PROPERTY, current_uri, lang, datatype, reify_uri, "", "", true);
		} else {
			self->_stack.emplace_back(ElementType::PROPERTY, current_uri, lang, datatype, reify_uri, "", "", false);
		}
	}
}

void RdfXmlParser::onEndElement(void *ctx, const xmlChar *localname, const xmlChar *prefix, const xmlChar *URI) {
	auto *self = static_cast<RdfXmlParser *>(ctx);
	if (self->_stack.empty())
		return;

	auto &top = self->_stack.back();
	if (top.type == ElementType::PROPERTY_XML_LITERAL && top.literal_depth > 0) {
		std::ostringstream oss;
		oss << "</";
		if (prefix)
			oss << (const char *)prefix << ":";
		oss << (const char *)localname << ">";
		top.text_buf += oss.str();
		top.literal_depth--;
		return;
	}

	auto current_uri = self->expandUri(URI, localname);
	if (current_uri == self->RDF_NS + "RDF") {
		self->_stack.pop_back();
		return;
	}

	ElementFrame current = std::move(self->_stack.back());
	self->_stack.pop_back();

	if (current.type == ElementType::PROPERTY_COLLECTION) {
		if (current.collection_tail.empty()) {
			self->emit(self->_stack.back().uri, current.uri, self->NIL_URI, "", "");
		} else {
			self->emit(current.collection_tail, self->REST_URI, self->NIL_URI, "", "");
		}
	} else if ((current.type == ElementType::PROPERTY || current.type == ElementType::PROPERTY_XML_LITERAL) &&
	           !current.has_obj_nodes) {
		auto text = self->trim(current.text_buf);
		auto dt = (current.type == ElementType::PROPERTY_XML_LITERAL) ? self->RDF_NS + "XMLLiteral" : current.datatype;
		auto lit_lang = dt.empty() ? current.lang : "";
		if (!self->_stack.empty()) {
			self->emitWithReification(self->_stack.back().uri, current.uri, text, dt, lit_lang, current.reify_id);
		}
	}
}

void RdfXmlParser::onCharacters(void *ctx, const xmlChar *ch, int len) {
	auto *self = static_cast<RdfXmlParser *>(ctx);
	if (!self->_stack.empty()) {
		auto &frame = self->_stack.back();
		if (frame.type == ElementType::PROPERTY) {
			frame.text_buf.append((const char *)ch, len);
		} else if (frame.type == ElementType::PROPERTY_XML_LITERAL) {
			frame.text_buf.append(xmlEscape((const char *)ch, len));
		}
	}
}

void RdfXmlParser::emitWithReification(const std::string &s, const std::string &p, const std::string &o,
                                       const std::string &dt, const std::string &lang, const std::string &r_id) {
	emit(s, p, o, dt, lang);
	if (!r_id.empty()) {
		emit(r_id, RDF_NS + "type", RDF_NS + "Statement", "", "");
		emit(r_id, RDF_NS + "subject", s, "", "");
		emit(r_id, RDF_NS + "predicate", p, "", "");
		emit(r_id, RDF_NS + "object", o, dt, lang);
	}
}

void RdfXmlParser::emit(const std::string &s, const std::string &p, const std::string &o, const std::string &dt,
                        const std::string &lang) {
	on_statement({s, p, o, dt, lang});
}

void RdfXmlParser::emit(const std::string &s, const std::string &p, const LibXMLView &o, const std::string &dt,
                        const std::string &lang) {
	on_statement({s, p, o.toString(), dt, lang});
}

void RdfXmlParser::emit(const std::string &s, const std::string &p, const LibXMLView &o, const LibXMLView &dt,
                        const std::string &lang) {
	on_statement({s, p, o.toString(), dt.toString(), lang});
}

void RdfXmlParser::emit(const std::string &s, const std::string &p, const LibXMLView &o, const LibXMLView &dt,
                        const LibXMLView &lang) {
	on_statement({s, p, o.toString(), dt.toString(), lang.toString()});
}

std::string RdfXmlParser::expandUri(const xmlChar *URI, const xmlChar *localname) {
	return URI ? std::string((const char *)URI) + (const char *)localname : (const char *)localname;
}

std::string RdfXmlParser::trim(const std::string &s) {
	size_t first = s.find_first_not_of(" \t\n\r");
	return (first == std::string::npos) ? "" : s.substr(first, s.find_last_not_of(" \t\n\r") - first + 1);
}
