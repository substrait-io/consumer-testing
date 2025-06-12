import os
from pathlib import Path
from sys import platform

import jpype

ISTHMUS_JARS = Path(__file__).parent.parent.parent / "jars" / "*"


the_java_home = "CONDA_PREFIX"
if "JAVA_HOME" in os.environ:
    the_java_home = "JAVA_HOME"

java_home_path = os.environ[the_java_home]
jvm_path = java_home_path

if not os.path.isfile(jvm_path):
    if platform == "darwin":
        jvm_path = java_home_path + "/lib/libjli.dylib"
        jpype.startJVM("--enable-preview", convertStrings=True, jvmpath=jvm_path)
    elif platform == "linux":
        jvm_path = java_home_path + "/lib/server/libjvm.so"
        if not jpype.isJVMStarted():
            jpype.startJVM(convertStrings=True, jvmpath=jvm_path)

jpype.addClassPath(ISTHMUS_JARS)

ArrayListClass = jpype.JClass("java.util.ArrayList")
ListClass = jpype.JClass("java.util.List")
SqlToSubstraitClass = jpype.JClass("io.substrait.isthmus.SqlToSubstrait")
VersionClass = jpype.JClass("io.substrait.plan.Plan")
