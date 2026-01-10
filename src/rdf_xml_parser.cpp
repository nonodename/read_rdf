#include "include/rdf_xml_parser.hpp"
#include <libxml/uri.h>
#include <sstream>

RdfXmlParser::RdfXmlParser(StatementCallback s_cb, NamespaceCallback n_cb, ErrorCallback e_cb, std::string base)
    : on_statement(s_cb), on_namespace(n_cb), on_error(e_cb), base_uri(base), bnode_count(0),
      _ctxt(nullptr, &xmlFreeParserCtxt) {
	setupSAX();

	xmlParserCtxtPtr ctxt = xmlCreatePushParserCtxt(&saxHandler, this, nullptr, 0, nullptr);
	_ctxt.reset(ctxt);
}

void RdfXmlParser::parseChunk(const char *chunk, int size, bool is_final) {
	if (_at_eof) { // permit multiple safe calls with is_final true
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
	xmlURI *uri_parsed = xmlParseURI(uri.c_str());
	bool has_scheme = false;
	has_scheme = (uri_parsed != NULL && uri_parsed->scheme != NULL);
	xmlFreeURI(uri_parsed);
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
	return xmlEscape(const_cast<char *>(data.c_str()), static_cast<int>(data.size()));
}

std::string RdfXmlParser::xmlEscape(const char *data, int len) {
	std::ostringstream escaped;
	for (int i = 0; i < len; ++i) {
		switch (data[i]) {
		case '&':
			escaped << "&amp;";
			break;
		case '<':
			escaped << "&lt;";
			break;
		case '>':
			escaped << "&gt;";
			break;
		case '\"':
			escaped << "&quot;";
			break;
		case '\'':
			escaped << "&apos;";
			break;
		default:
			escaped << data[i];
			break;
		}
	}
	return escaped.str();
}

std::string RdfXmlParser::literalXML(const xmlChar *localname, const xmlChar *prefix, const xmlChar **namespaces,
                                     int nb_namespaces, const xmlChar **attributes, int nb_attributes) {
	// TODO handle namespaces per the example
	std::ostringstream oss;
	oss << "<";
	if (prefix)
		oss << (const char *)prefix << ":";
	oss << (const char *)localname;

	// Namespaces: nb_namespaces is the count of pairs
	for (int i = 0; i < nb_namespaces; ++i) {
		oss << " xmlns";
		if (namespaces[i * 2])
			oss << ":" << (const char *)namespaces[i * 2];
		oss << "=\\\"" << (const char *)namespaces[i * 2 + 1] << "\\\"";
	}

	// Attributes: attributes are 5-tuple (localname, prefix, URI, value_start, value_end)
	for (int i = 0; i < nb_attributes; ++i) {
		oss << " " << (const char *)attributes[i * 5];
		std::string val((const char *)attributes[i * 5 + 3],
		                (const char *)attributes[i * 5 + 4] - (const char *)attributes[i * 5 + 3]);
		oss << "=\\\"" << xmlEscape(val) << "\\\"";
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

// TODO add support for rdf:li elements which resolve to rdf:_1, rdf:_2, etc.
void RdfXmlParser::onStartElement(void *ctx, const xmlChar *localname, const xmlChar *prefix, const xmlChar *URI,
                                  int nb_namespaces, const xmlChar **namespaces, int nb_attributes, int nb_defaulted,
                                  const xmlChar **attributes) {
	auto *self = static_cast<RdfXmlParser *>(ctx);

	ElementFrame *parent_frame = self->_stack.empty() ? nullptr : &(self->_stack.back());
	if (parent_frame && parent_frame->is_xml_literal) {
		parent_frame->literal_depth++;
		parent_frame->text_buf +=
		    self->literalXML(localname, prefix, namespaces, nb_namespaces, attributes, nb_attributes);
		return;
	}
	for (int i = 0; i < nb_namespaces; ++i) {
		std::string prefix_str;
		if (namespaces[i * 2]) {
			prefix_str = std::string((const char *)namespaces[i * 2]);
		} else {
			prefix_str = "";
		}
		std::string uri_str = (const char *)namespaces[i * 2 + 1];
		self->on_namespace(prefix_str, uri_str);
	}

	std::string current_uri = self->expandUri(URI, localname);
	if (current_uri == self->RDF_NS + "RDF") {
		ElementFrame ef = {ElementType::ROOT,
		                   "",
		                   self->findAttr(nb_attributes, attributes, self->XML_NS, self->LANG_TAG),
		                   "",
		                   "",
		                   "",
		                   self->findAttr(nb_attributes, attributes, self->XML_NS, self->BASE_TAG),
		                   false,
		                   false};
		self->_stack.push_back(ef);
		return;
	}

	ElementType parent_type = parent_frame ? parent_frame->type : ElementType::ROOT;
	ElementType current_type = (parent_type == ElementType::PROPERTY || parent_type == ElementType::ROOT)
	                               ? ElementType::NODE
	                               : ElementType::PROPERTY;

	auto about = self->findAttr(nb_attributes, attributes, self->RDF_NS, self->ABOUT_ATTR);
	auto nodeID = self->findAttr(nb_attributes, attributes, self->RDF_NS, self->NODE_ID_ATTR);
	auto rdf_id = self->findAttr(nb_attributes, attributes, self->RDF_NS, self->ID_ATTR);
	auto resource = self->findAttr(nb_attributes, attributes, self->RDF_NS, self->RESOURCE_ATTR);
	auto datatype = self->findAttr(nb_attributes, attributes, self->RDF_NS, self->DATATYPE_ATTR);
	auto parseType = self->findAttr(nb_attributes, attributes, self->RDF_NS, self->PARSE_TYPE_ATTR);
	auto lang = self->findAttr(nb_attributes, attributes, self->XML_NS, self->LANG_TAG);
	auto base = self->findAttr(nb_attributes, attributes, self->XML_NS, self->BASE_TAG);

	if (lang.empty() && parent_frame)
		lang = parent_frame->lang;
	if (base.empty()) {
		if (parent_frame)
			base = parent_frame->baseURI;
		else
			base = self->base_uri;
	}

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
		if (parent_type == ElementType::PROPERTY) {
			parent_frame->has_obj_nodes = true;
			ElementFrame prop = std::move(self->_stack.back());
			self->_stack.pop_back();
			self->emitWithReification(self->_stack.back().uri, prop.uri, subject, "", "", prop.reify_id);
			self->_stack.push_back(std::move(prop));
		}
		if (current_uri != self->RDF_NS + "Description")
			self->emit(subject, self->RDF_NS + "type", current_uri, "", "");
		self->processAttributes(nb_attributes, attributes, subject, lang);
		self->_stack.push_back({ElementType::NODE, subject, lang, "", "", "", base, false, false});
	} else { // PROPERTY
		auto reify_uri = rdf_id.empty() ? "" : self->currentBaseURI() + "#" + rdf_id;

		// Check for Property Attributes
		bool has_prop_attrs = false;
		for (int i = 0; i < nb_attributes; ++i) {
			if (!self->isReservedAttr(self->expandUri(attributes[i * 5 + 2], attributes[i * 5]))) {
				has_prop_attrs = true;
				break;
			}
		}

		if (parseType == "Resource") {
			auto bnode = self->generateBNode();
			self->emitWithReification(parent_frame->uri, current_uri, bnode, "", "", reify_uri);
			self->_stack.push_back({ElementType::NODE, bnode, lang, "", "", "", "", false, false});
		} else if (!resource.empty() || !nodeID.empty() || (has_prop_attrs && parseType.empty())) {
			// This is an "Empty Property Element"
			if (!resource.empty() && !isAbsolute(resource)) {
				resource = self->currentBaseURI() + resource;
			}
			auto object = !resource.empty() ? resource : (!nodeID.empty() ? "_:" + nodeID : self->generateBNode());
			self->emitWithReification(parent_frame->uri, current_uri, object, "", "", reify_uri);
			// Emit triples for all property attributes on this object
			self->processAttributes(nb_attributes, attributes, object, lang);
			self->_stack.push_back(
			    {ElementType::PROPERTY, current_uri, lang, datatype, reify_uri, "", "", true, false});
		} else {
			auto is_xml = (parseType == "Literal");
			self->_stack.push_back(
			    {ElementType::PROPERTY, current_uri, lang, datatype, reify_uri, "", "", false, is_xml});
		}
	}
}

void RdfXmlParser::onEndElement(void *ctx, const xmlChar *localname, const xmlChar *prefix, const xmlChar *URI) {
	auto *self = static_cast<RdfXmlParser *>(ctx);
	if (self->_stack.empty())
		return;
	if (self->_stack.back().is_xml_literal && self->_stack.back().literal_depth > 0) {
		auto &frame = self->_stack.back();
		std::ostringstream oss;
		oss << "</";
		if (prefix)
			oss << (const char *)prefix << ":";
		oss << (const char *)localname << ">";

		frame.text_buf += oss.str();
		frame.literal_depth--;
		return;
	}
	auto current_uri = self->expandUri(URI, localname);
	if (current_uri == self->RDF_NS + "RDF") {
		self->_stack.pop_back();
		return;
	}

	ElementFrame current = std::move(self->_stack.back());
	self->_stack.pop_back();

	if (current.type == ElementType::PROPERTY && !current.has_obj_nodes) {
		auto text = self->trim(current.text_buf);
		auto dt = current.is_xml_literal ? self->RDF_NS + "XMLLiteral" : current.datatype;
		auto lit_lang = dt.empty() ? current.lang : "";
		if (!self->_stack.empty()) {
			self->emitWithReification(self->_stack.back().uri, current.uri, text, dt, lit_lang, current.reify_id);
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

void RdfXmlParser::onCharacters(void *ctx, const xmlChar *ch, int len) {
	auto *self = static_cast<RdfXmlParser *>(ctx);
	if (!self->_stack.empty() && self->_stack.back().type == ElementType::PROPERTY) {
		if (self->_stack.back().is_xml_literal) {
			self->_stack.back().text_buf.append(xmlEscape((const char *)ch, len));
		} else {
			self->_stack.back().text_buf.append((const char *)ch, len);
		}
	}
}

std::string RdfXmlParser::findAttr(int nb_attributes, const xmlChar **attributes, const std::string &ns,
                                   const std::string &local) {
	for (int i = 0; i < nb_attributes; ++i) {
		if (local == (const char *)attributes[i * 5] &&
		    (attributes[i * 5 + 2] && ns == (const char *)attributes[i * 5 + 2])) {
			return std::string((const char *)attributes[i * 5 + 3],
			                   (const char *)attributes[i * 5 + 4] - (const char *)attributes[i * 5 + 3]);
		}
	}
	return "";
}

std::string RdfXmlParser::expandUri(const xmlChar *URI, const xmlChar *localname) {
	return URI ? std::string((const char *)URI) + (const char *)localname : (const char *)localname;
}

std::string RdfXmlParser::trim(const std::string &s) {
	size_t first = s.find_first_not_of(" \t\n\r");
	return (first == std::string::npos) ? "" : s.substr(first, s.find_last_not_of(" \t\n\r") - first + 1);
}
