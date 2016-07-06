#include <gtest/gtest.h>

#include "common/Properties.h"
#include "tests/integtest.h"

#include <string>
#include <stdlib.h>

using std::cout;
using std::endl;
using std::string;
using KFS::Properties;
using KFS::Test::QFSTestUtils;

TEST(LoadPropertiesTest, LoadFromEnvVariable) {

	string confStr = "fieldA = $QFS_ENV_VAR_FIELD_A\n";
	confStr       += "fieldB = $QFS_ENV_VAR_FIELD_B\n";

	int ret = setenv("QFS_ENV_VAR_FIELD_A", "field_value", 1);
	if(ret == -1) {
		cout << "Can't set the environment variable. Test is skipped." << endl;
		return;
	}

	string filePath = QFSTestUtils::WriteTempFile(confStr);
	Properties p;
	p.loadProperties(filePath.c_str(), '=');
	ASSERT_STREQ(p.getValue("fieldA", ""), "field_value");
	ASSERT_STREQ(p.getValue("fieldB", ""), "");

	QFSTestUtils::RemoveForcefully(filePath);
}
