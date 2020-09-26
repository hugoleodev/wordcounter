compile:
	@sbt compile

package:
	@sbt package

test:
	@sbt test

generatejar: compile package

run: generatejar
	@spark-submit --class dev.hugoleonardo.WordCounterJOB target/scala-2.12/wordcounter_2.12-0.1.jar
