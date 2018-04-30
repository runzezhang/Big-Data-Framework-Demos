1. (kafka file location)/bin/zookeeper-server-start.sh config/zookeeper.properties

2. (kafka file location)/bin/kafka-server-start.sh config/server.properties

3. (kafka file location)/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitterstream

4. python app.py

5. (kafka file location)/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic twitterstream --from-beginning

6. python streamConsumer.py

sample result:
/home/parallels/anaconda3/bin/python /home/parallels/PycharmProjects/HW3/q4/streamConsumer.py
Ivy Default Cache set to: /home/parallels/.ivy2/cache
The jars for the packages stored in: /home/parallels/.ivy2/jars
:: loading settings :: url = jar:file:/home/parallels/anaconda3/lib/python3.6/site-packages/pyspark/jars/ivy-2.4.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
org.apache.spark#spark-streaming-kafka-0-8_2.11 added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent;1.0
	confs: [default]
	found org.apache.spark#spark-streaming-kafka-0-8_2.11;2.0.2 in central
	found org.apache.kafka#kafka_2.11;0.8.2.1 in central
	found org.scala-lang.modules#scala-xml_2.11;1.0.2 in central
	found com.yammer.metrics#metrics-core;2.2.0 in central
	found org.slf4j#slf4j-api;1.7.16 in central
	found org.scala-lang.modules#scala-parser-combinators_2.11;1.0.2 in central
	found com.101tec#zkclient;0.3 in central
	found log4j#log4j;1.2.17 in central
	found org.apache.kafka#kafka-clients;0.8.2.1 in central
	found net.jpountz.lz4#lz4;1.3.0 in central
	found org.xerial.snappy#snappy-java;1.1.2.6 in central
	found org.apache.spark#spark-tags_2.11;2.0.2 in central
	found org.scalatest#scalatest_2.11;2.2.6 in central
	found org.scala-lang#scala-reflect;2.11.8 in central
	found org.spark-project.spark#unused;1.0.0 in central
:: resolution report :: resolve 2168ms :: artifacts dl 20ms
	:: modules in use:
	com.101tec#zkclient;0.3 from central in [default]
	com.yammer.metrics#metrics-core;2.2.0 from central in [default]
	log4j#log4j;1.2.17 from central in [default]
	net.jpountz.lz4#lz4;1.3.0 from central in [default]
	org.apache.kafka#kafka-clients;0.8.2.1 from central in [default]
	org.apache.kafka#kafka_2.11;0.8.2.1 from central in [default]
	org.apache.spark#spark-streaming-kafka-0-8_2.11;2.0.2 from central in [default]
	org.apache.spark#spark-tags_2.11;2.0.2 from central in [default]
	org.scala-lang#scala-reflect;2.11.8 from central in [default]
	org.scala-lang.modules#scala-parser-combinators_2.11;1.0.2 from central in [default]
	org.scala-lang.modules#scala-xml_2.11;1.0.2 from central in [default]
	org.scalatest#scalatest_2.11;2.2.6 from central in [default]
	org.slf4j#slf4j-api;1.7.16 from central in [default]
	org.spark-project.spark#unused;1.0.0 from central in [default]
	org.xerial.snappy#snappy-java;1.1.2.6 from central in [default]
	---------------------------------------------------------------------
	|                  |            modules            ||   artifacts   |
	|       conf       | number| search|dwnlded|evicted|| number|dwnlded|
	---------------------------------------------------------------------
	|      default     |   15  |   2   |   2   |   0   ||   15  |   0   |
	---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent
	confs: [default]
	0 artifacts copied, 15 already retrieved (0kB/16ms)
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
17/12/04 23:27:16 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
17/12/04 23:27:16 WARN Utils: Your hostname, ubuntu resolves to a loopback address: 127.0.1.1; using 10.211.55.6 instead (on interface enp0s5)
17/12/04 23:27:16 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
RT @ShaunKing: Never forget this.
negative


Ever.
negative


This isn’t just Trump.
negative


The entire Republican Party has decided to hitch their wagon to a…
negative


RT @semajrabnud: @kenvogel @AJentleson i know russians who have less contact with russians than the trump org., et al, do
negative


RT @SaraDoesITBest_: damn trump tryna take away net neutrality and birth control he want me to b bored Nd pregnant like im living on a farm…
negative


RT @benshapiro: So the same anti-Trump agent (1) signed the opening of the Russia investigation; (2) changed the language on Comey’…
negative


RT @correctthemedia: The FBI has an $8.6 billion annual budget and over 35,000 employees, but one dude that's a Trump-hater
1) opened t…
negative


RT @Evan_McMullin: The clearest example yet of Trump's effect on the GOP.
negative


What message does this send to women, victims of sexual assa…
positive


RT @USAloveGOD: #Holder, #Comey fight Trump's #FBI slam: 'Not letting this go'
We the #American people feel the @FBI have cheated u…
negative


RT @SusanNow3: Trump just endorsed Roy Moore,  a man who thinks:

1.
negative


Gay people should be put in jail.
positive


2.
negative


Women shouldn't hold pu…
negative


We can only hope It is fake news?
negative


https://t.co/rYWwoh0v83
negative


RT @TheRickyDavila: Boom.
negative


🔥👏🔥👏🔥 https://t.co/kuv1GqfeLe
negative


RT @pastpunditry: As with Trump, GOP officials distanced themselves from Moore not bc they cared about the assaults on girls and wome…
negative


RT @JohnHuckleberry: Alec Baldwin's Trump Visited by Ghosts of Trump's Past in 'SNL' Cold Open -… https://t.co/fQSXn4k4jo
negative


Trump Mulls Creating Private Spy Network - Democratic Underground https://t.co/mbjGVMFDen via @demunderground
negative


RT @SteveBannen: I love watching Obama lecturing the world on climate change while we throw his 'individual mandate' out the window...
positive


RT @thehill: JUST IN: RNC reinstates support for Roy Moore after Trump endorsement https://t.co/pYrh9MhA6R https://t.co/pX8V9gtL0d
negative


RT @chuckwoolery: Clinton Aides Went Unpunished After Making False Statements To Anti-Trump FBI Supervisor https://t.co/CCfrWMOC9Z via @dai…
negative


RT @ChristiChat: More Winning!
positive


SCOTUS decides 7-2 that it is perfectly legal to ban people, who hate US,  from entering America.
negative


Me…
negative


@Puddyta30634464 @deplorable_pooh @Czen420 @Fuctupmind She probably kicks Obama's ass on a regular basis.
negative


😁
negative


RT @JoyAnnReid: Important to remember that among the biggest losers in what Trump did today in Utah, gutting the Antiquities Act, a…
positive


@Kimberlee373 @karmelaje @realDonaldTrump @VP ✨👍🇺🇸WINNING!🇺🇸👍✨

👍❤️Love it!Thank you @POTUS Trump!
negative


✨👍🇺🇸WINNING🇺🇸👍✨
negative


RT @funder: Here are the only known photos of Ivanka, Don Jr &amp; Eric Trump in Russia together.
negative


Please RT &amp; share to the ends of…
negative


RT @dbongino: Let me get this straight, a subordinate sends out a mistaken tweet &amp; Trump is guilty of obstruction of justice but…
negative


RT @Khanoisseur: Amyntor Group, based in Whitefish Montana, is one of the companies involved in the private rendition and spy networ…
negative


RT @flightcrew: @JackPosobiec He’s coming.
negative


They Tried To Steal His Presidency, Smear His Name, And Threaten His Children With Man…
negative


RT @RealSaavedra: FBI Agent Peter Strzok:

-Fired for documented anti-Trump bias
-Interviewed Hillary &amp; changed language in report to…
negative


RT @ClaraJeffery: 1) Trump uses Navajo Code Talkers as political prop to lash out at opponent
2) Trump deprives Code Talkers of their…
negative


RT @amjoyshow: RNC jumps back in Alabama Senate Race after #Trump endorses #RoyMoore https://t.co/0yFV9CA2R3 via @nbcnews
negative


RT @Imperator_Rex3: A big win, but what a waste of time.
positive


The lower court judges should be charged with obstructing justice &amp; invoiced p…
negative


RT @michaeldweiss: President endorses accused child molester for senator.
negative


https://t.co/USM7mCiOcU
negative


RT @RepSwalwell: Let’s be real.
positive


@realDonaldTrump is undecided as to where Israel is on a map.
negative


https://t.co/r2mueuOVhk
negative


RT @nancygolliday: The "T" word can go "F" itself.
negative


https://t.co/tZYZtbwh58
negative


RT @AdamParkhomenko: 2/2 since you are no longer going to sleep tonight, you can read more here: https://t.co/Qt8ok0Auw8 https://t.co/sakql…
positive


RT @StockMonsterVIP: BREAKING: Some of Peter Strzok Anti-Trump hate texts were being sent to his
FBI girlfriend !!
negative


Wonder how Strzok wif…
negative


RT @COPicard2017: Getting mad yet?
negative


Stopping the #GOPTaxScam would be a good form of retaliation.
positive


Make some calls, send some emails, r…
negative


RT @AltMtRainier: RT if you agree.
negative


@realDonaldTrump is a madman.
negative


https://t.co/y06BOrOBhU
negative


The truth matters.
negative


Accusations matter.
negative


Sexual harassment matters.
positive


Climate change matters.
negative


Gun reform matters.
negative


Do… https://t.co/bDlhi4DaF6
negative


RT @LouDobbs: #LDTPoll: Do you believe FBI Agent Peter Strzok’s Anti-Trump, Pro-Clinton activities are the clearest evidence yet…
negative


RT @SethAbramson: PS/ NOTE: these are bad, stupid criminals (in the case of Trump, Pence, Flynn &amp;c) working with bad, stupid lackeys…
negative


CAROLINE O
@RVAwonk

#TrumpPutinPuppet
Told The Truth TWICE

1st Time...
Trump
Admitted Sexual Assults
To Billy Bus… https://t.co/yL5ahZfJLI
positive


RT @DCSamantha: “The decision to reduce the size of the Monument is being made with no tribal consultation.
negative


The Navajo Nation will…
negative


A protest in Trump Country brings home nation's race divides https://t.co/bKfZ5onZGL
negative


Wow Trump even made the Moon Great Again!
positive


👍 https://t.co/mEkRvSMZfN
negative


How do people sleep at night knowing they voted for trump 😂💀
negative


RT @feministabulous: Man accused of sexual assault by multiple women endorses man accused of sexual assault by multiple women.
positive


https://t.co…
negative


RT @PalmerReport: Paul Manafort got out on bail and immediately conspired with the Russians again.
negative


Nixon hired burglars who couldn’t…
negative


RT @SafetyPinDaily: These are all the women who have accused Donald Trump of sexual assault   |Via Independent https://t.co/ulxFsRY4qM
positive


Trump Declares Massive Downsizing of National Monuments in Utah https://t.co/OGLjAshbqt
negative


@SenFeinstein @SenBlumenthal @RepAdamSchiff @RepSwalwell @SenSchumer @NancyPelosi @SpeakerRyan @SenateMajLdr WHAT T… https://t.co/wJLM5x2fN8
negative


And before he took office, too!
negative


He and his "gorgeous" daughter run/own sweat shops in China, Mexico, and Pakistan,… https://t.co/KKn2gplOFJ
positive


Trump takes rare step to reduce 2 national monuments in Utah https://t.co/GcvVScWXPw via @Yahoo
positive


RT @LindaSuhler: You got CRUSHED in what was an imminently winnable election, Mittens &amp; doomed us to 4 more yrs of Obama.
positive


You're wea…
negative


RT @TheTweetOfGod: I know for a fact that Donald Trump has a very small penis.
negative


Please retweet this enough that he can't help but respond.…
negative


RT @shannynmoore: Please watch and support my friend @SamSeder - he is a gift to truth.
negative


He was right about Polanski.
positive


He’s right about…
positive


RT @StefanMolyneux: “I want the American people to know this truth: The FBI is honest.
positive


The FBI is strong.
positive


And the FBI is, and always wi…
negative


RT @AIIAmericanGirI: BOOM!
negative


Sara Carter: There Are Other Anti-Trump Mueller Team Emails Out There "A Lot More Is Going to Come Out" (VIde…
positive


RT @TheTweetOfGod: I know for a fact that Donald Trump has a very small penis.
negative


Please retweet this enough that he can't help but respond.…
negative


RT @SaysHummingbird: Never in my life have I seen a President reach rock bottom in moral bankruptcy &amp; shameless indecency while at the s…
negative


RT @fabianzg_: that will be taken away from the public due to the desire to open mining, oil rigging, etc.
negative


donald trump made a mon…
negative


RT @jk_rowling: It might help to see other examples of John Dowd's carefully crafted prose.
negative


If they too are randomly capitalised, b…
negative


RT @RealJack: Nobody has ever seen President Trump drink alcohol or do drugs in his entire life and now the media and Democrats a…
negative


Excellent explainer on why Trump/Flynn's undermining of US diplomatic signals was so significant.
positive


https://t.co/MhXaBXNSsD
negative


RT @TheMuddyCuck: Sounds like an excellent reason to do it, then.
positive


https://t.co/ZofMbGMBZf
negative


RT @JackPosobiec: McCabe lied to Flynn to get him into the meeting under false pretenses and did not allow him access to a lawyer and…
negative


RT @NRC_Egeland: Forget Trump, Brexit and the World Cup.
negative


The way men with guns and power play with 20 million civilian lives in…
negative


RT @amplifirenews_: Protesters SHUT DOWN Congressional hallways to FIGHT BACK against the Trump tax scam!
negative


https://t.co/XY4ViwYkWy
negative


Retweeted Mr.
negative


Weeks (@MrDane1982):

When Mueller is done with Donald Trump, everything he has done needs to be... https://t.co/xymjl3rzWY
negative


RT @rabiasquared: Trump does not care https://t.co/ogcny7FdQX
negative


RT @julieturkewitz: *VERY* important to note: This isn’t just about one monument.
positive


Trump's decision is expected to trigger a legal battl…
neutral


RT @TrickFreee: “It is a direct-action arm, totally off the books,” meaning the intelligence collected would not be shared with the…
negative


RT @itsRJHill: That’s why these dumbass Americans voted for Trump https://t.co/zvyg64IbZp
negative


Trump may face a reckoning in case brought by female accuser https://t.co/yJofE13b4V ..
negative