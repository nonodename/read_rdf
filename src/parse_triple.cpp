#include "parse_triple.hpp"
#include <cstdint>
#include <cctype>

bool ParseTripleLine(const std::string &line, std::string &subject, std::string &predicate, std::string &object,
                     std::string &lang_tag, std::string &datatype_iri) {
	subject.clear();
	predicate.clear();
	object.clear();
	lang_tag.clear();
	datatype_iri.clear();

	enum class State {
		Start,
		SubjectIRI,
		SubjectBlank,
		PredicateIRI,
		ObjectIRI,
		ObjectBlank,
		ObjectLiteral,
		ObjectLiteralEscaped,
		AfterLiteral,
		LangTag,
		DatatypeIRI,
		End
	};

	const char *p = line.data();
	const char *end = p + line.size();

	auto skip_ws = [&]() {
		while (p < end && isspace(static_cast<unsigned char>(*p)))
			++p;
	};

	auto append_utf8_codepoint = [&](uint32_t cp) {
		if (cp <= 0x7F)
			object.push_back(static_cast<char>(cp));
		else if (cp <= 0x7FF) {
			object.push_back(static_cast<char>(0xC0 | ((cp >> 6) & 0x1F)));
			object.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
		} else if (cp <= 0xFFFF) {
			object.push_back(static_cast<char>(0xE0 | ((cp >> 12) & 0x0F)));
			object.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
			object.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
		} else {
			object.push_back(static_cast<char>(0xF0 | ((cp >> 18) & 0x07)));
			object.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
			object.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
			object.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
		}
	};

	auto parse_hex = [&](const char *pos, size_t length, uint32_t &out) -> bool {
		out = 0;
		if (pos + length > end)
			return false;
		for (size_t j = 0; j < length; ++j) {
			char hc = pos[j];
			uint32_t val;
			if (hc >= '0' && hc <= '9')
				val = hc - '0';
			else if (hc >= 'A' && hc <= 'F')
				val = hc - 'A' + 10;
			else if (hc >= 'a' && hc <= 'f')
				val = hc - 'a' + 10;
			else
				return false;
			out = (out << 4) | val;
		}
		return true;
	};

	skip_ws();
	State state = State::Start;
	const char *token_start = nullptr;

	while (p < end) {
		char c = *p;
		switch (state) {
		case State::Start:
			if (c == '<') {
				state = State::SubjectIRI;
				++p;
				token_start = p;
			} else if (c == '_' && p + 1 < end && *(p + 1) == ':') {
				state = State::SubjectBlank;
				p += 2;
				token_start = p;
			} else
				return false;
			break;

		case State::SubjectIRI:
			if (c == '>') {
				subject.assign(token_start, p - token_start);
				++p;
				skip_ws();
				if (p >= end || *p != '<')
					return false;
				++p;
				state = State::PredicateIRI;
				token_start = p;
			} else
				++p;
			break;

		case State::SubjectBlank:
			if (isspace(static_cast<unsigned char>(c))) {
				subject.assign(token_start, p - token_start);
				skip_ws();
				if (p >= end || *p != '<')
					return false;
				++p;
				state = State::PredicateIRI;
				token_start = p;
			} else
				++p;
			break;

		case State::PredicateIRI:
			if (c == '>') {
				predicate.assign(token_start, p - token_start);
				++p;
				skip_ws();
				if (p >= end)
					return false;
				if (*p == '<') {
					state = State::ObjectIRI;
					++p;
					token_start = p;
				} else if (*p == '_' && p + 1 < end && *(p + 1) == ':') {
					state = State::ObjectBlank;
					p += 2;
					token_start = p;
				} else if (*p == '"') {
					state = State::ObjectLiteral;
					++p;
					object.clear();
				} else
					return false;
			} else
				++p;
			break;

		case State::ObjectIRI:
			if (c == '>') {
				object.assign(token_start, p - token_start);
				state = State::End;
				++p;
			} else
				++p;
			break;

		case State::ObjectBlank:
			if (isspace(static_cast<unsigned char>(c)) || c == '.') {
				object.assign(token_start, p - token_start);
				state = State::End;
			} else
				++p;
			break;

		case State::ObjectLiteral:
			if (c == '\\') {
				state = State::ObjectLiteralEscaped;
				++p;
			} else if (c == '"') {
				state = State::AfterLiteral;
				++p;
			} else {
				object.push_back(c);
				++p;
			}
			break;

		case State::ObjectLiteralEscaped:
			if (c == 'u') {
				uint32_t cp;
				if (!parse_hex(p + 1, 4, cp))
					return false;
				append_utf8_codepoint(cp);
				p += 5;
			} else if (c == 'U') {
				uint32_t cp;
				if (!parse_hex(p + 1, 8, cp))
					return false;
				append_utf8_codepoint(cp);
				p += 9;
			} else {
				object.push_back(c);
				++p;
			}
			state = State::ObjectLiteral;
			break;

		case State::AfterLiteral:
			if (c == '@') {
				state = State::LangTag;
				++p;
				token_start = p;
			} else if (c == '^' && p + 1 < end && *(p + 1) == '^') {
				p += 2;
				skip_ws();
				if (p >= end || *p != '<')
					return false;
				++p;
				state = State::DatatypeIRI;
				token_start = p;
			} else
				state = State::End;
			break;

		case State::LangTag:
			if (isspace(static_cast<unsigned char>(c)) || c == '.') {
				lang_tag.assign(token_start, p - token_start);
				state = State::End;
			} else
				++p;
			break;

		case State::DatatypeIRI:
			if (c == '>') {
				datatype_iri.assign(token_start, p - token_start);
				state = State::End;
				++p;
			} else
				++p;
			break;

		case State::End:
			skip_ws();
			if (p < end && *p == '.')
				++p;
			skip_ws();
			return true;
		}
	}

	return state == State::End || state == State::ObjectIRI || state == State::ObjectBlank;
}
