--  Para correr los tests deben cargar en hugs el módulo Tests
--  y evaluar la expresión "main".
-- Algunas funciones que pueden utilizar para chequear resultados:
-- http://hackage.haskell.org/package/hspec-expectations-0.6.1/docs/Test-Hspec-Expectations.html#t:Expectation

import Test.Hspec
import MapReduce

main :: IO ()
main = hspec $ do
  describe "Utilizando Diccionarios" $ do
    it "belongs, (?)" $ do
      belongs 3 [(3, "A"), (0, "R"), (7, "G")]    `shouldBe` True
      belongs "k" []                              `shouldBe` False
      [("H", [1]), ("E", [2]), ("Y", [0])] ? "R"  `shouldBe` False
      [("V", [1]), ("O", [2]), ("S", [0])] ? "V"  `shouldBe` True
      [("calle",[3]),("city",[2,1])] ? "city"     `shouldBe` True
    
    it "get, (!)" $ do
      get 3 [(3, "A"), (0, "R"), (7, "G")]        `shouldBe` "A"
      [("V", [1]), ("O", [2]), ("S", [0])] ! "V"  `shouldBe` [1]
      [("calle",[3]),("city",[2,1])] ! "city"     `shouldBe` [2,1]
    
    it "insertWith" $ do
      insertWith (++) 2 ['p'] (insertWith (++) 1 ['a','b'] (insertWith (++) 1 ['l'] []))
      `shouldMatchList` [(1,"lab"),(2,"p")]
    
    it "groupByKey" $ do
      groupByKey [("calle","Jean Jaures"),("ciudad","Brujas"),("ciudad","Kyoto"),("calle","7")]
      `shouldMatchList` [("calle",["Jean Jaures","7"]),("ciudad",["Brujas","Kyoto"])]

    it "unionWith" $ do
      unionWith (++) [("calle",[3]),("city",[2,1])] [("calle", [4]), ("altura", [1,3,2])]
      `shouldMatchList` [("calle",[3,4]),("city",[2,1]),("altura",[1,3,2])]
  
    it "distributionProcess" $ do
      distributionProcess 5 [1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12]
      `shouldBe` [[1 ,6 ,11] ,[2 ,7 ,12] ,[3 ,8] ,[4 ,9] ,[5 ,10]]

  describe "Utilizando Map Reduce" $ do
    it "visitas por monumento funciona en algún orden" $ do
      visitasPorMonumento [ "m1" ,"m2" ,"m3" ,"m2","m1", "m3", "m3"] `shouldMatchList` [("m3",3), ("m1",2), ("m2",2)]

    it "monumentosTop devuelve los más visitados en algún orden" $ do 
      monumentosTop [ "m1", "m0", "m0", "m0", "m2", "m2", "m3"] 
      `shouldSatisfy` (\res -> res == ["m0", "m2", "m3", "m1"] || res == ["m0", "m2", "m1", "m3"])