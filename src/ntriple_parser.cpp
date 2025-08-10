#include <iostream>
#include <fstream>
#include <string>
#include <cctype>

using namespace std;

// --- FSM parser ---
static bool ParseTripleLine(const string &line,
                             string &subject,
                             string &predicate,
                             string &object,
                             string &lang_tag,
                             string &datatype_iri) {
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

    size_t i = 0;
    const size_t n = line.size();

    auto skip_ws = [&]() {
        while (i < n && isspace(static_cast<unsigned char>(line[i]))) i++;
    };

    auto append_utf8_codepoint = [&](uint32_t cp) {
        if (cp <= 0x7F) {
            object.push_back(static_cast<char>(cp));
        } else if (cp <= 0x7FF) {
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

    auto parse_hex = [&](size_t start, size_t length, uint32_t &out) -> bool {
        out = 0;
        if (start + length > n) return false;
        for (size_t j = 0; j < length; j++) {
            char hc = line[start + j];
            uint32_t val;
            if (hc >= '0' && hc <= '9') val = hc - '0';
            else if (hc >= 'A' && hc <= 'F') val = hc - 'A' + 10;
            else if (hc >= 'a' && hc <= 'f') val = hc - 'a' + 10;
            else return false;
            out = (out << 4) | val;
        }
        return true;
    };

    skip_ws();
    State state = State::Start;

    while (i < n) {
        char c = line[i];

        switch (state) {
            case State::Start:
                if (c == '<') {
                    state = State::SubjectIRI;
                    i++;
                } else if (c == '_' && i + 1 < n && line[i + 1] == ':') {
                    state = State::SubjectBlank;
                    i += 2;
                } else {
                    return false;
                }
                break;

            case State::SubjectIRI:
                if (c == '>') {
                    i++;
                    skip_ws();
                    if (i >= n || line[i] != '<') return false;
                    i++;
                    state = State::PredicateIRI;
                } else {
                    subject.push_back(c);
                    i++;
                }
                break;

            case State::SubjectBlank:
                if (isspace(static_cast<unsigned char>(c))) {
                    skip_ws();
                    if (i >= n || line[i] != '<') return false;
                    i++;
                    state = State::PredicateIRI;
                } else {
                    subject.push_back(c);
                    i++;
                }
                break;

            case State::PredicateIRI:
                if (c == '>') {
                    i++;
                    skip_ws();
                    if (i >= n) return false;
                    if (line[i] == '<') {
                        state = State::ObjectIRI;
                        i++;
                    } else if (line[i] == '_' && i + 1 < n && line[i + 1] == ':') {
                        state = State::ObjectBlank;
                        i += 2;
                    } else if (line[i] == '"') {
                        state = State::ObjectLiteral;
                        i++;
                    } else {
                        return false;
                    }
                } else {
                    predicate.push_back(c);
                    i++;
                }
                break;

            case State::ObjectIRI:
                if (c == '>') {
                    state = State::End;
                    i++;
                } else {
                    object.push_back(c);
                    i++;
                }
                break;

            case State::ObjectBlank:
                if (isspace(static_cast<unsigned char>(c)) || c == '.') {
                    state = State::End;
                } else {
                    object.push_back(c);
                    i++;
                }
                break;

            case State::ObjectLiteral:
                if (c == '\\') {
                    state = State::ObjectLiteralEscaped;
                    i++;
                } else if (c == '"') {
                    state = State::AfterLiteral;
                    i++;
                } else {
                    object.push_back(c);
                    i++;
                }
                break;

            case State::ObjectLiteralEscaped:
                if (c == 'u') {
                    uint32_t cp;
                    if (!parse_hex(i + 1, 4, cp)) return false;
                    append_utf8_codepoint(cp);
                    i += 5; // 'u' + 4 hex
                } else if (c == 'U') {
                    uint32_t cp;
                    if (!parse_hex(i + 1, 8, cp)) return false;
                    append_utf8_codepoint(cp);
                    i += 9; // 'U' + 8 hex
                } else {
                    // normal escape
                    object.push_back(c);
                    i++;
                }
                state = State::ObjectLiteral;
                break;

            case State::AfterLiteral:
                if (c == '@') {
                    state = State::LangTag;
                    i++;
                } else if (c == '^' && i + 1 < n && line[i + 1] == '^') {
                    i += 2;
                    skip_ws();
                    if (i >= n || line[i] != '<') return false;
                    i++;
                    state = State::DatatypeIRI;
                } else {
                    state = State::End;
                }
                break;

            case State::LangTag:
                if (isspace(static_cast<unsigned char>(c)) || c == '.') {
                    state = State::End;
                } else {
                    lang_tag.push_back(c);
                    i++;
                }
                break;

            case State::DatatypeIRI:
                if (c == '>') {
                    state = State::End;
                    i++;
                } else {
                    datatype_iri.push_back(c);
                    i++;
                }
                break;

            case State::End:
                skip_ws();
                if (i < n && line[i] == '.') i++;
                skip_ws();
                return true;
        }
    }

    return state == State::End || state == State::ObjectIRI || state == State::ObjectBlank;
}

// --- Main test program ---
int main(int argc, char *argv[]) {
    if (argc != 2) {
        cerr << "Usage: " << argv[0] << " file.nt\n";
        return 1;
    }

    ifstream infile(argv[1]);
    if (!infile.is_open()) {
        cerr << "Error: Could not open file " << argv[1] << "\n";
        return 1;
    }

    string subject, predicate, object, lang_tag, datatype_iri;
    string line;
    size_t line_num = 0;

    while (getline(infile, line)) {
        line_num++;
        if (line.empty() || line[0] == '#') {
            continue; // skip empty/comment lines
        }

        bool ok = ParseTripleLine(line, subject, predicate, object, lang_tag, datatype_iri);
        if (!ok) {
            cout << "Line " << line_num << ": Parse failed\n";
        } else {
            cout << "Line " << line_num << ":\n";
            cout << "  subject : " << subject << "\n";
            cout << "  predicate: " << predicate << "\n";
            cout << "  object  : " << object << "\n";
            if (!lang_tag.empty())
                cout << "  lang_tag: " << lang_tag << "\n";
            if (!datatype_iri.empty())
                cout << "  datatype: " << datatype_iri << "\n";
        }
        cout << "---------------------------------\n";
    }

    return 0;
}
