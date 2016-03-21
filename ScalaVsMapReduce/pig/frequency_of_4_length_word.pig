-- Your	code	should	output	the	top	10	most	commonly	occurring	words	and	their	counts	for	four	 word	lengths.	It	is	up	to	you	how	you	implement	this	– either	have	the	word length	as	a	parameter	or	hard	code	it

-- Please provide path to the merged input file.

data =LOAD 'hdfs://cshadoop1/user/kpw150030/README.md';
allWords = foreach data GENERATE flatten(TOKENIZE((chararray)$0)) as word;


--We are interested only in word length of 4 - hence putting the condition

allFilteredWords =  FILTER allWords BY SIZE(*) == 4;
groupedWord = group allFilteredWords by word;
wordCount = foreach groupedWord generate $0,COUNT(allFilteredWords);

-- sort and return top 10
sortedWordCounts =  ORDER wordCount BY $1 DESC;
topTenCounts = LIMIT sortedWordCounts  10;
dump topTenCounts;