cat excluded | while read line ;do

echo "assemblyExcludedJars in assembly ++= {  " >> ttt
echo "  val cp = (fullClasspath in assembly).value" >>ttt
echo "  cp filter {_.data.getName == \"$line\"}" >> ttt
echo "}" >> ttt
echo "" >> ttt
done

cat ttt >> build.sbt
rm -f ttt

