import sys
import env
import itertools
import cppgenerator
import code.querytemplate
import code.fetch

from pprint import pprint

def numRows(relation,env):
	notFound = "Relation " + relation + " does not exist in database."
	if relation in env.schemas:
		schema = env.schemas[relation]
		ordering = schema["orderings"][0]
		trieName = relation + "_" + "_".join(map(str,ordering))
		types = map(lambda x : str(x["attrType"]),schema["attributes"])
		if trieName in env.relations:
			if env.relations[trieName] == "disk":
				#get encodings
				result = cppgenerator.compileAndRun(lambda: 
					fetchData(relation,trieName,schema["annotation"],schema["attributes"],env),
					"fetchData_"+relation,env.config["memory"],types,schema["annotation"])
				return result[0].num_rows(result[1])
			else:
				print "Not yet supported. Must be on disk."
		else:
			print notFound
	else:
		print notFound
	return -1

def fetch(relation,env):
	notFound = "Relation " + relation + " does not exist in database."
	if relation in env.schemas:
		schema = env.schemas[relation]
		ordering = schema["orderings"][0]
		trieName = relation + "_" + "_".join(map(str,ordering))
		types = map(lambda x : str(x["attrType"]),schema["attributes"])
		if trieName in env.relations:
			if env.relations[trieName] == "disk":
				#get encodings
				hashstring = "fetchData_"+relation
				result = cppgenerator.compileAndRun(
					f=lambda:fetchData(relation,trieName,schema["annotation"],schema["attributes"],env),
					hashstring=hashstring,
					mem=env.config["memory"],
					types=types,
					annotationType=schema["annotation"],
					numThreads=str(env.config["numThreads"]))
				tuples = eval("""result[0].fetch_data_"""+str(hashstring)+"""(result[1])""")
				return tuples
			else:
				print "Not yet supported. Must be on disk."
		else:
			print notFound
	else:
		print notFound
	return -1

def fetchData(relation,trieName,annotationType,attributes,env):
	memType = env.config["memory"]
	include = """#include "utils/thread_pool.hpp"
	#include "Trie.hpp"
	#include "utils/timer.hpp"
	#include "utils/%(memType)s.hpp"
	"""% locals()
	path = env.config["database"] + "/relations/" + relation + "/" + trieName
	runCode = code.fetch.loadRelation(path,trieName,annotationType,memType)
	
	s = set()
	for a in attributes:
		if a["encoding"] not in s:
			runCode += code.fetch.loadEncoding(env.config["database"],a["encoding"],a["attrType"])
			s.add(a["encoding"])
	encodings = map(lambda i:i["encoding"],attributes)
	hashstring = "fetchData_"+relation
	runCode += code.fetch.setResult(trieName,encodings,hashstring)
	return code.querytemplate.getCode(include,runCode,hashstring)
	#cppgenerator.compileAndRun(
	#lambda: cppgenerator.loadRelations(relations,env),
	#libname)
	#print relation