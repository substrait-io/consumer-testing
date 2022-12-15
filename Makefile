all:
	cd ${SUBSTRAIT_JAVA_HOME}/isthmus; ../gradlew shadowJar
	mkdir -p jars
	cp ${SUBSTRAIT_JAVA_HOME}/isthmus/build/libs/*all.jar ./jars/

clean:
	rm -r jars
