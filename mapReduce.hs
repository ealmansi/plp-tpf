module MapReduce where

import Data.Ord
import Data.List

-- ---------------------------------Sección 1---------Diccionario ---------------------------
type Dict k v = [(k,v)]

-- Ejercicio 1
belongs :: Eq k => k -> Dict k v -> Bool
belongs k d = elem k $ map fst d

(?) :: Eq k => Dict k v -> k -> Bool
(?) d k = belongs k d

-- Ejercicio 2
get :: Eq k => k -> Dict k v -> v
get k d = snd $ head $ filter (\kv -> (fst kv) == k) d

(!) :: Eq k => Dict k v -> k -> v
(!) d k = get k d

-- Ejercicio 3
insertWith :: Eq k => (v -> v -> v) -> k -> v -> Dict k v -> Dict k v
insertWith f k v d
  | d ? k       = map (\kv -> newValue kv) d
  | otherwise   = (k, v) : d
  where
    newValue kv = ((fst kv), if (fst kv) == k then f (snd kv) v else (snd kv))

-- Ejercicio 4
groupByKey :: Eq k => [(k,v)] -> Dict k [v]
groupByKey xs = [(k, valuesOf k) | k <- keys]
  where
    keys = nub $ map fst xs
    valuesOf k = map snd $ filter (\kv -> (fst kv) == k) xs

-- Ejercicio 5
unionWith :: Eq k => (v -> v -> v) -> Dict k v -> Dict k v -> Dict k v
unionWith f d1 d2 = [(k, valueOf k) | k <- union (keys d1) (keys d2)]
  where
    keys d = map fst d
    valueOf k
      | (d1 ? k) && (d2 ? k)  = f (d1 ! k) (d2 ! k)
      | (d1 ? k)              = (d1 ! k)
      | otherwise             = (d2 ! k)

-- ------------------------------Sección 2--------------MapReduce---------------------------

type Mapper a k v = a -> [(k,v)]
type Reducer k v b = (k, [v]) -> [b]

-- Ejercicio 6
distributionProcess :: Int -> [a] -> [[a]]
distributionProcess = undefined

-- Ejercicio 7
mapperProcess :: Eq k => Mapper a k v -> [a] -> [(k,[v])]
mapperProcess = undefined

-- Ejercicio 8
combinerProcess :: (Eq k, Ord k) => [[(k, [v])]] -> [(k,[v])]
combinerProcess = undefined

-- Ejercicio 9
reducerProcess :: Reducer k v b -> [(k, [v])] -> [b]
reducerProcess = undefined

-- Ejercicio 10
mapReduce :: (Eq k, Ord k) => Mapper a k v -> Reducer k v b -> [a] -> [b]
mapReduce = undefined

-- Ejercicio 11
visitasPorMonumento :: [String] -> Dict String Int
visitasPorMonumento = undefined

-- Ejercicio 12
monumentosTop :: [String] -> [String]
monumentosTop = undefined

-- Ejercicio 13 
monumentosPorPais :: [(Structure, Dict String String)] -> [(String, Int)]
monumentosPorPais = undefined


-- ------------------------ Ejemplo de datos del ejercicio 13 ----------------------
data Structure = Street | City | Monument deriving Show

items :: [(Structure, Dict String String)]
items = [
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


------------------------------------------------
------------------------------------------------