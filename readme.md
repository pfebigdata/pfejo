# projet pfeJo
commands for maven version:

mvn archetype:generate -B    -DarchetypeGroupId=net.alchim31.maven -DarchetypeArtifactId=scala-archetype-simple -DarchetypeVersion=1.7   -DgroupId=pfejo -DartifactId=project -Dversion=0.1-SNAPSHOT -Dpackage=pfejo

mvn scala:compile
mvn scala:run -DmainClass=pfejo.App
mvn scala:run -X -DmainClass=pfejo.App
