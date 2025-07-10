#define DUCKDB_EXTENSION_MAIN

#include "read_rdf_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void ReadRdfScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "ReadRdf " + name.GetString() + " 🐥");
	});
}

inline void ReadRdfOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "ReadRdf " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(DatabaseInstance &instance) {
	// Register a scalar function
	auto read_rdf_scalar_function = ScalarFunction("read_rdf", {LogicalType::VARCHAR}, LogicalType::VARCHAR, ReadRdfScalarFun);
	ExtensionUtil::RegisterFunction(instance, read_rdf_scalar_function);

	// Register another scalar function
	auto read_rdf_openssl_version_scalar_function = ScalarFunction("read_rdf_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, ReadRdfOpenSSLVersionScalarFun);
	ExtensionUtil::RegisterFunction(instance, read_rdf_openssl_version_scalar_function);
}

void ReadRdfExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string ReadRdfExtension::Name() {
	return "read_rdf";
}

std::string ReadRdfExtension::Version() const {
#ifdef EXT_VERSION_READ_RDF
	return EXT_VERSION_READ_RDF;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void read_rdf_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::ReadRdfExtension>();
}

DUCKDB_EXTENSION_API const char *read_rdf_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
