--  Para correr los tests deben cargar en hugs el módulo Tests
--  y evaluar la expresión "main".
-- Algunas funciones que pueden utilizar para chequear resultados:
-- http://hackage.haskell.org/package/hspec-expectations-0.6.1/docs/Test-Hspec-Expectations.html#t:Expectation

import Test.Hspec
import Data.List
import MapReduce

-- ------------------------ Ejemplo de datos para el ejercicio 13 ----------------------

items1 :: [(Structure, Dict String String)]
items1 = [
    (Monument, [
      ("name","Obelisco"),
      ("latlong","-36.6033,-57.3817"),
      ("country", "Argentina")]),
    (Street, [
      ("name","Int. Güiraldes"),
      ("latlong","-34.5454,-58.4386"),
      ("country", "Argentina")]),
    (Monument, [
      ("name", "San Martín"),
      ("country", "Argentina"),
      ("latlong", "-34.6033,-58.3817")]),
    (City, [
      ("name", "Paris"),
      ("country", "Francia"),
      ("latlong", "-24.6033,-18.3817")]),
    (Monument, [
      ("name", "Bagdad Bridge"),
      ("country", "Irak"),
      ("new_field", "new"),
      ("latlong", "-11.6033,-12.3817")])
    ]

items2 :: [(Structure, Dict String String)]
items2 = [
    ]

items3 :: [(Structure, Dict String String)]
items3 = [
    (Monument, [
      ("name","Obelisco"),
      ("latlong","-36.6033,-57.3817"),
      ("country", "Argentina")]),
    (Monument, [
      ("name","Obelisco"),
      ("latlong","-36.6033,-57.3817"),
      ("country", "Brasil")]),
    (Monument, [
      ("name","Obelisco"),
      ("latlong","-36.6033,-57.3817"),
      ("country", "Yugoslavia")]),
    (Monument, [
      ("name","Obelisco"),
      ("latlong","-36.6033,-57.3817"),
      ("country", "Argentina")]),
    (Street, [
      ("name","Int. Güiraldes"),
      ("latlong","-34.5454,-58.4386"),
      ("country", "Argentina")]),
    (Monument, [
      ("name", "San Martín"),
      ("country", "Argentina"),
      ("latlong", "-34.6033,-58.3817")]),
    (City, [
      ("name", "Paris"),
      ("country", "Francia"),
      ("latlong", "-24.6033,-18.3817")]),
    (City, [
      ("name", "Paris"),
      ("country", "Francia"),
      ("latlong", "-24.6033,-18.3817")]),
    (Monument, [
      ("name", "Bagdad Bridge"),
      ("country", "Yugoslavia"),
      ("new_field", "new"),
      ("latlong", "-11.6033,-12.3817")])
    ]

-- ------------------------ Tests ----------------------

main :: IO ()
main = hspec $ do
  describe "Utilizando Diccionarios" $ do
    it "belongs, (?)" $ do
      belongs 3 [(3, "A"), (0, "R"), (7, "G")]
        `shouldBe` True
      belongs "k" []
        `shouldBe` False
      [("H", [1]), ("E", [2]), ("Y", [0])] ? "R"
        `shouldBe` False
      [("V", [1]), ("O", [2]), ("S", [0])] ? "V"
        `shouldBe` True
      [("calle",[3]),("city",[2,1])] ? "city"
        `shouldBe` True
    
    it "get, (!)" $ do
      get 3 [(3, "A"), (0, "R"), (7, "G")]
        `shouldBe` "A"
      [("V", [1]), ("O", [2]), ("S", [0])] ! "V"
        `shouldBe` [1]
      [("calle",[3]),("city",[2,1])] ! "city"
        `shouldBe` [2,1]
    
    it "insertWith" $ do
      insertWith (++) 1 [99] [(1 , [1]) , (2 , [2])]
        `shouldMatchList` [(1 ,[1 ,99]) ,(2 ,[2])]
      insertWith (++) 3 [99] [(1 , [1]) , (2 , [2])]
        `shouldMatchList` [(1 ,[1]) ,(2 ,[2]) ,(3 ,[99])]
      insertWith (++) 2 ['p'] (insertWith (++) 1 ['a','b'] (insertWith (++) 1 ['l'] []))
        `shouldMatchList` [(1,"lab"),(2,"p")]
    
    it "groupByKey" $ do
      groupByKey [("calle","Jean Jaures"),("ciudad","Brujas"),("ciudad","Kyoto"),("calle","7")]
        `shouldMatchList` [("calle",["Jean Jaures","7"]),("ciudad",["Brujas","Kyoto"])]
      groupByKey [("10",4),("33",756),("10",32),("95",76),("33",-68),("10",777)]
        `shouldMatchList` [("10",[4,32,777]), ("33",[756,-68]), ("95",[76])]
      groupByKey (groupByKey (groupByKey [("10",4), ("10",45)]))
        `shouldMatchList` [("10",[[[4,45]]])]

    it "unionWith" $ do
      unionWith (++) [("calle",[3]),("city",[2,1])] [("calle", [4]), ("altura", [1,3,2])]
        `shouldMatchList` [("calle",[3,4]),("city",[2,1]),("altura",[1,3,2])]
      unionWith (+) [("calle",23),("city",654)] [("calle", -23), ("altura", 435), ("city", -1)]
        `shouldMatchList` [("calle",0), ("city",653), ("altura",435)]
      unionWith union [("calle",[0]), ("city",[653]), ("altura",[435])] [("calle",[3,4]),("city",[2,1]),("altura",[1,3,2])]
        `shouldMatchList` [("calle",[0,3,4]), ("city",[653,2,1]), ("altura",[435,1,3,2])]

  describe "Utilizando Map Reduce" $ do
    it "distributionProcess" $ do
      distributionProcess 5 [1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12]
        `shouldBe` [[1 ,6 ,11] ,[2 ,7 ,12] ,[3 ,8] ,[4 ,9] ,[5 ,10]]
      distributionProcess 2 [1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12]
        `shouldBe` [[1,3,5,7,9,11],[2,4,6,8,10,12]]
      distributionProcess 97 [1..150]
        `shouldBe` [[1,98],[2,99],[3,100],[4,101],[5,102],[6,103],[7,104],
          [8,105],[9,106],[10,107],[11,108],[12,109],[13,110],[14,111],[15,112],
          [16,113],[17,114],[18,115],[19,116],[20,117],[21,118],[22,119],[23,120],
          [24,121],[25,122],[26,123],[27,124],[28,125],[29,126],[30,127],[31,128],
          [32,129],[33,130],[34,131],[35,132],[36,133],[37,134],[38,135],[39,136],
          [40,137],[41,138],[42,139],[43,140],[44,141],[45,142],[46,143],[47,144],
          [48,145],[49,146],[50,147],[51,148],[52,149],[53,150],[54],[55],[56],[57],
          [58],[59],[60],[61],[62],[63],[64],[65],[66],[67],[68],[69],[70],[71],[72],
          [73],[74],[75],[76],[77],[78],[79],[80],[81],[82],[83],[84],[85],[86],[87],
          [88],[89],[90],[91],[92],[93],[94],[95],[96],[97]]
    
    it "mapperProcess" $ do
      mapperProcess (\xs -> [(x, 1) | x <- xs]) [[1 ,6 ,11] ,[6 ,7 ,12] ,[12, 8] ,[11 ,9] ,[1 ,10]]
        `shouldBe` [(1,[1,1]),(6,[1,1]),(11,[1,1]),(7,[1]),(12,[1,1]),(8,[1]),(9,[1]),(10,[1])]
      mapperProcess (\xs -> [(product xs, sum xs)]) [[1 ,6 ,11] ,[6 ,7 ,12] ,[12, 8] ,[11 ,9] ,[66, 1]]
        `shouldBe` [(66,[18,67]),(504,[25]),(96,[20]),(99,[20])]
    
    it "combinerProcess" $ do
      combinerProcess [[(1,[1,1]),(66,[1,1]),(11,[1,1]),(7,[1]),(99,[1,1]),(8,[1]),(9,[1]),(10,[1])],
                      [(66,[18,67]),(504,[25]),(96,[20]),(99,[20])]]
        `shouldBe` [(1,[1,1]),(66,[1,1,18,67]),(11,[1,1]),(7,[1]),(99,[1,1,20]),
                    (8,[1]),(9,[1]),(10,[1]),(504,[25]),(96,[20])]
    
    it "reducerProcess" $ do
      reducerProcess (\(k, vs) -> k:vs) [(1,[1,1]),(66,[1,1,18,67]),(11,[1,1]),(7,[1]),(99,[1,1,20]),
                                (8,[1]),(9,[1]),(10,[1]),(504,[25]),(96,[20])]
        `shouldBe` [1,1,1,66,1,1,18,67,11,1,1,7,1,99,1,1,20,8,1,9,1,10,1,504,25,96,20]
      
    it "visitasPorMonumento" $ do
      visitasPorMonumento [ "m1" ,"m2" ,"m3" ,"m2","m1", "m3", "m3"]
        `shouldMatchList` [("m3",3), ("m1",2), ("m2",2)]
      visitasPorMonumento [ "m1" ,"m2" ,"m3" ,"m2","m1", "m3", "m3", "m1" ,"m2" ,"m3" ,"m2","m1", "m3", "m3"]
        `shouldMatchList` [("m3",6), ("m1",4), ("m2",4)]
      visitasPorMonumento ([ "m2" ,"m2" ,"m3" ,"m2","m1", "m3", "m2"] ++ ["m100" | i <- [1..100]])
        `shouldMatchList` [("m2",4), ("m100",100), ("m3",2), ("m1",1)]

    it "monumentosTop" $ do 
      monumentosTop [ "m1", "m0", "m0", "m0", "m2", "m2", "m3"] 
        `shouldSatisfy` (\res -> res == ["m0", "m2", "m3", "m1"] || res == ["m0", "m2", "m1", "m3"])
      monumentosTop ([ "m2" ,"m2" ,"m3" ,"m2","m1", "m3", "m2"] ++ ["m100" | i <- [1..100]])
        `shouldBe` ["m100","m2","m3","m1"]

    it "monumentosPorPais" $ do 
      monumentosPorPais items1
        `shouldMatchList` [("Argentina", 2), ("Irak", 1)]
      monumentosPorPais items2
        `shouldMatchList` []
      monumentosPorPais items3
        `shouldMatchList` [("Argentina",3), ("Brasil",1), ("Yugoslavia",2)]