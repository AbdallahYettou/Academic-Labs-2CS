""" /GEOPOINT;ARRONDISSEMENT;GENRE;ESPECE;FAMILLE;ANNEE PLANTATION;HAUTEUR;CIRCONFERENCE;ADRESSE;NOM COMMUN;VARIETE;OBJECTID;NOM_EV
(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly, avenue de La Motte-Piquet, avenue de la Bourdonnais, avenue de Suffren;Oranger des Osages;;6;Parc du Champs de Mars
(48.8685686134, 2.31331809304);8;Calocedrus;decurrens;Cupressaceae;1854;20.0;195.0;Cours-la-Reine, avenue Franklin-D.-Roosevelt, avenue Matignon, avenue Gabriel;Cèdre à encens;;11;Jardin des Champs Elysées
(48.8768191638, 2.33210374339);9;Pterocarya;fraxinifolia;Juglandaceae;1862;22.0;330.0;Place d'Estienne-d'Orves;Pérocarya du Caucase;;14;Square Etienne d'Orves
(48.8373323894, 2.40776275516);12;Celtis;australis;Cannabaceae;1906;16.0;295.0;27, boulevard Soult;Micocoulier de Provence;;16;Avenue 27 boulevard Soult
(48.8341842636, 2.46130493573);12;Quercus;petraea;Fagaceae;1784;30.0;430.0;route ronde des Minimes;Chêne rouvre;;19;Bois de Vincennes (lac des minimes)
(48.8325900983, 2.41116455985);12;Platanus;x acerifolia;Platanaceae;1860;45.0;405.0;Ile de Bercy;Platane commun;;21;Bois de Vincennes (Ile de Bercy)
(48.8226749117, 2.33869560229);14;Platanus;x acerifolia;Platanaceae;1840;40.0;580.0;Bd Jourdan, avenue Reille, rue Gazan, rue de la Cité‚-Universitaire, rue Nansouty;Platane commun;;26;Parc Montsouris """
#Q1 
from pyspark.sql import SparkSession
sc = SparkSession.builder.appName("Lap2").getOrCreate()
RDD= sc.textFile("hdfs://master:9000/arbres.csv")
number_of_lines = RDD.count()

print("Number of lines in the file: ", number_of_lines)
#Q2
avg_height = RDD.map(lambda line: float(line.split(";")[6])).mean()
print("Average height of the trees: ", avg_height)
#Q3
""" (3) Display  the  genus  of  the  tallest  tree:  The  principle  is  to  construct  key-value  pairs, 
where  the  tree  heights  serve  as  the  key  and  their  genus  as  the  value.  Then,  sort  the 
pairs in descending order by key using sortByKey and keep only the first pair """

en_tete = RDD.first()
RDD_sans_entete = RDD.filter(lambda ligne: ligne != en_tete)
pairs = RDD_sans_entete.map(lambda line: (float(line.split(";")[6]), line.split(";")[2]))
pairs_sorted = pairs.sortByKey(ascending=False)
print("Average height of the trees: ", pairs_sorted.first())

#Q4
""" 
(4) Display the number of trees for each genus: The principle is to construct a pair (genus, 1) for each tree in the file, then aggregate the values by genus using reduceByKey. """


pairs = RDD_sans_entete.map(lambda line: (line.split(";")[2], 1))

reducepairs = pairs.reduceByKey(lambda a, b: a + b)

resultats = reducepairs.collect()

for genre, compte in resultats:
    print(f"Genus: {genre} -> Number of trees: {compte}")