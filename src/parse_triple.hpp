#pragma once
#include <string>

bool ParseTripleLine(const std::string &line, std::string &subject, std::string &predicate, std::string &object,
                     std::string &lang_tag, std::string &datatype_iri);
