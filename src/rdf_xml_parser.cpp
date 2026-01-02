#include "include/rdf_xml_parser.hpp"

RdfXmlParser::RdfXmlParser(StatementCallback s_cb, NamespaceCallback n_cb, ErrorCallback e_cb, std::string base)
    : on_statement(s_cb), on_namespace(n_cb), on_error(e_cb), base_uri(base), bnode_count(0),
      _ctxt(nullptr, &xmlFreeParserCtxt) {
	setupSAX();
	xmlParserCtxtPtr ctxt = xmlCreatePushParserCtxt(&saxHandler, this, nullptr, 0, nullptr);
	_ctxt.reset(ctxt);
}

void RdfXmlParser::parseChunk(const char *chunk, int size, bool is_final) {
	if (!_ctxt) {
		on_error("Failed to create XML parser context");
		return;
	}
	if (xmlParseChunk(_ctxt.get(), chunk, size, is_final) != 0) {
		on_error("XML parsing error");
	}
}
RdfXmlParser::~RdfXmlParser() {
}
std::string RdfXmlParser::generateBNode() {
	return "_:b" + std::to_string(++bnode_count);
}

bool RdfXmlParser::isReservedAttr(const std::string &uri) {
	if (uri.find(XML_NS) == 0)
		return true;
	if (uri.find(RDF_NS) != 0)
		return false;
	std::string local = uri.substr(RDF_NS.length());
	return (local == "about" || local == "ID" || local == "nodeID" || local == "resource" || local == "datatype" ||
	        local == "parseType");
}

void RdfXmlParser::setupSAX() {
	memset(&saxHandler, 0, sizeof(saxHandler));
	saxHandler.initialized = XML_SAX2_MAGIC;
	saxHandler.startElementNs = &RdfXmlParser::onStartElement;
	saxHandler.endElementNs = &RdfXmlParser::onEndElement;
	saxHandler.characters = &RdfXmlParser::onCharacters;
}

void RdfXmlParser::onStartElement(void *ctx, const xmlChar *localname, const xmlChar *prefix, const xmlChar *URI,
                                  int nb_namespaces, const xmlChar **namespaces, int nb_attributes, int nb_defaulted,
                                  const xmlChar **attributes) {
	auto *self = static_cast<RdfXmlParser *>(ctx);
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
		                   self->findAttr(nb_attributes, attributes, self->XML_NS, "lang"),
		                   "",
		                   "",
		                   "",
		                   false,
		                   false};
		self->stack.push(ef);
		return;
	}

	ElementType parent_type = self->stack.empty() ? ElementType::ROOT : self->stack.top().type;
	ElementType current_type = (parent_type == ElementType::PROPERTY || parent_type == ElementType::ROOT)
	                               ? ElementType::NODE
	                               : ElementType::PROPERTY;

	std::string about = self->findAttr(nb_attributes, attributes, self->RDF_NS, "about");
	std::string nodeID = self->findAttr(nb_attributes, attributes, self->RDF_NS, "nodeID");
	std::string rdf_id = self->findAttr(nb_attributes, attributes, self->RDF_NS, "ID");
	std::string resource = self->findAttr(nb_attributes, attributes, self->RDF_NS, "resource");
	std::string datatype = self->findAttr(nb_attributes, attributes, self->RDF_NS, "datatype");
	std::string parseType = self->findAttr(nb_attributes, attributes, self->RDF_NS, "parseType");
	std::string lang = self->findAttr(nb_attributes, attributes, self->XML_NS, "lang");
	if (lang.empty() && !self->stack.empty())
		lang = self->stack.top().lang;

	if (current_type == ElementType::NODE) {
		std::string subject;
		if (!about.empty())
			subject = about;
		else if (!rdf_id.empty())
			subject = self->base_uri + "#" + rdf_id;
		else if (!nodeID.empty())
			subject = "_:" + nodeID;
		else
			subject = self->generateBNode();

		if (parent_type == ElementType::PROPERTY) {
			self->stack.top().has_obj_nodes = true;
			ElementFrame prop = std::move(self->stack.top());
			self->stack.pop();
			self->emitWithReification(self->stack.top().uri, prop.uri, subject, "", "", prop.reify_id);
			self->stack.push(std::move(prop));
		}
		if (current_uri != self->RDF_NS + "Description")
			self->emit(subject, self->RDF_NS + "type", current_uri, "", "");

		for (int i = 0; i < nb_attributes; ++i) {
			std::string attr_uri = self->expandUri(attributes[i * 5 + 2], attributes[i * 5]);
			if (!self->isReservedAttr(attr_uri)) {
				std::string val((const char *)attributes[i * 5 + 3],
				                (const char *)attributes[i * 5 + 4] - (const char *)attributes[i * 5 + 3]);
				self->emit(subject, attr_uri, val, "", lang);
			}
		}
		self->stack.push({ElementType::NODE, subject, lang, "", "", "", false, false});
	} else { // PROPERTY
		std::string reify_uri = rdf_id.empty() ? "" : self->base_uri + "#" + rdf_id;

		// Check for Property Attributes
		bool has_prop_attrs = false;
		for (int i = 0; i < nb_attributes; ++i) {
			if (!self->isReservedAttr(self->expandUri(attributes[i * 5 + 2], attributes[i * 5]))) {
				has_prop_attrs = true;
				break;
			}
		}

		if (parseType == "Resource") {
			std::string bnode = self->generateBNode();
			self->emitWithReification(self->stack.top().uri, current_uri, bnode, "", "", reify_uri);
			self->stack.push({ElementType::NODE, bnode, lang, "", "", "", false, false});
		} else if (!resource.empty() || !nodeID.empty() || (has_prop_attrs && parseType.empty())) {
			// This is an "Empty Property Element"
			std::string object =
			    !resource.empty() ? resource : (!nodeID.empty() ? "_:" + nodeID : self->generateBNode());
			self->emitWithReification(self->stack.top().uri, current_uri, object, "", "", reify_uri);

			// Emit triples for all property attributes on this object
			for (int i = 0; i < nb_attributes; ++i) {
				std::string attr_uri = self->expandUri(attributes[i * 5 + 2], attributes[i * 5]);
				if (!self->isReservedAttr(attr_uri)) {
					std::string val((const char *)attributes[i * 5 + 3],
					                (const char *)attributes[i * 5 + 4] - (const char *)attributes[i * 5 + 3]);
					self->emit(object, attr_uri, val, "", lang);
				}
			}
			self->stack.push({ElementType::PROPERTY, current_uri, lang, datatype, reify_uri, "", true, false});
		} else {
			bool is_xml = (parseType == "Literal");
			self->stack.push({ElementType::PROPERTY, current_uri, lang, datatype, reify_uri, "", false, is_xml});
		}
	}
}

void RdfXmlParser::onEndElement(void *ctx, const xmlChar *localname, const xmlChar *prefix, const xmlChar *URI) {
	auto *self = static_cast<RdfXmlParser *>(ctx);
	if (self->stack.empty())
		return;
	std::string current_uri = self->expandUri(URI, localname);
	if (current_uri == self->RDF_NS + "RDF") {
		self->stack.pop();
		return;
	}

	ElementFrame current = std::move(self->stack.top());
	self->stack.pop();

	if (current.type == ElementType::PROPERTY && !current.has_obj_nodes) {
		std::string text = self->trim(current.text_buf);
		std::string dt = current.is_xml_literal ? self->RDF_NS + "XMLLiteral" : current.datatype;
		std::string lit_lang = dt.empty() ? current.lang : "";
		if (!self->stack.empty()) {
			self->emitWithReification(self->stack.top().uri, current.uri, text, dt, lit_lang, current.reify_id);
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
	if (!self->stack.empty() && self->stack.top().type == ElementType::PROPERTY) {
		self->stack.top().text_buf.append((const char *)ch, len);
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
