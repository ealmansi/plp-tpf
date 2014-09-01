module MapReduce where

import Data.Ord
import Data.List

-- ---------------------------------Sección 1---------Diccionario ---------------------------
type Dict k v = [(k,v)]

-- Ejercicio 1
belongs :: Eq k => k -> Dict k v -> Bool
belongs k d = elem k $ map fst $ d

(?) :: Eq k => Dict k v -> k -> Bool
(?) = flip belongs

-- Ejercicio 2
get :: Eq k => k -> Dict k v -> v
get k d = snd $ head $ filter (\kv -> (fst kv) == k) $ d

(!) :: Eq k => Dict k v -> k -> v
(!) = flip get

-- Ejercicio 3
insertWith :: Eq k => (v -> v -> v) -> k -> v -> Dict k v -> Dict k v
insertWith f k v d
  | d ? k       = map (\kv -> newValue kv) d
  | otherwise   = (k, v) : d
  where
    newValue kv = ((fst kv), if (fst kv) == k then f (snd kv) v else (snd kv))

-- Ejercicio 4
groupByKey :: Eq k => [(k,v)] -> Dict k [v]
groupByKey xs = [(k, valuesOf k) | k <- keys xs]
  where
    keys xs = nub $ map fst $ xs
    valuesOf k = map snd $ filter (\kv -> (fst kv) == k) $ xs

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
data Structure = Street | City | Monument deriving Show

-- Ejercicio 6
-- a cada elemento x de xs lo transforma en (i, x), donde i es el
-- bucket que le corresponde. luego, la respuesta es una comprensión
-- de n buckets, cada uno de ellos con los elementos que le corresponden.
distributionProcess :: Int -> [a] -> [[a]]
distributionProcess n xs = [map snd $ filter (\y -> (fst y) == i) $ ys | i <- [1..n]]
  where
    ys = zip [1 + mod (i - 1) n | i <- [1..length xs]] xs

-- Ejercicio 7
-- no estoy 100% seguro de que esto sea lo que pide el enunciado
mapperProcess :: Eq k => Mapper a k v -> [a] -> [(k,[v])]
mapperProcess mp xs = groupByKey $ concat $ map mp $ xs

-- Ejercicio 8
-- no estoy 100% seguro de que esto sea lo que pide el enunciado
combinerProcess :: (Eq k, Ord k) => [[(k, [v])]] -> [(k,[v])]
combinerProcess xs = foldr (\x r -> unionWith (++) x r) [] $ xs

-- Ejercicio 9
-- no estoy 100% seguro de que esto sea lo que pide el enunciado
reducerProcess :: Reducer k v b -> [(k, [v])] -> [b]
reducerProcess rd xs = concat $ foldr (\x r -> (rd x) : r) [] $ xs

-- Ejercicio 10
-- no estoy 100% seguro de que esto sea lo que pide el enunciado
mapReduce :: (Eq k, Ord k) => Mapper a k v -> Reducer k v b -> [a] -> [b]
mapReduce mp rd xs = reduced $ combined $ mapped $ distributed $ xs
  where
    distributed = distributionProcess 100
    mapped = map $ mapperProcess mp
    combined = combinerProcess
    reduced = reducerProcess rd

-- Ejercicio 11
visitasPorMonumento :: [String] -> Dict String Int
visitasPorMonumento = mapReduce mp rd
  where
    mp s = [(s, 1)]
    rd (k, vs) = [(k, length vs)]

-- Ejercicio 12
-- esto está feo, pero respeta el equema que pide el tp
monumentosTop :: [String] -> [String]
monumentosTop xs = mapReduce mp rd $ visitasPorMonumento $ xs
  where
    mp (s, i) = [("", (s, i))]
    rd (k, vs) = map fst $ sortBy (\a b -> compare (snd b) (snd a)) $ vs

-- Ejercicio 13 
monumentosPorPais :: [(Structure, Dict String String)] -> [(String, Int)]
monumentosPorPais = mapReduce mp rd
  where
    mp (Monument, dt) = [(dt ! "country", 1)]
    mp (_, dt) = []
    rd (k, vs) = [(k, length vs)]