module MapReduce where

import Data.Ord
import Data.List

-- ---------------------------------Sección 1: Diccionario------------------------------------
type Dict k v = [(k,v)]


-- Ejercicio 1

belongs :: Eq k => k -> Dict k v -> Bool
-- obtiene una lista con las keys del diccionario, y
-- verifica si la key provista es elemento de la lista

belongs k d = elem k $ map fst $ d

(?) :: Eq k => Dict k v -> k -> Bool
(?) = flip belongs


-- Ejercicio 2

get :: Eq k => k -> Dict k v -> v
-- precondicion: belongs k d == True
-- filtra todos los pares del diccionario donde la key no es la
-- key provista. en principio, el resultado podria tener 0 o 1 elementos,
-- pero dada la precondicion, el resultado tendra necesariamente 1 elemento.
-- al tomar head de la lista obtenida, se obtiene el par (k, v) correspondiente,
-- siendo el resultado final el value v

get k d = snd $ head $ filter (\kv -> (fst kv) == k) $ d

(!) :: Eq k => Dict k v -> k -> v
(!) = flip get


-- Ejercicio 3

insertWith :: Eq k => (v -> v -> v) -> k -> v -> Dict k v -> Dict k v
-- si la key no esta definida en el diccionario, simplemente se agrega el par
-- (k, v) al mismo. 
-- de lo contrario, se mapea cada value del diccionario a si mismo, salvo
-- el value de la key que se esta modificando, el cual recibe el nuevo valor
-- de f(value_anterior, value_nuevo)

insertWith f k v d
  | not $ d ? k     = (k, v) : d
  | otherwise       = map (\kv -> newValue kv) d
  where
    newValue kv = if (fst kv) /= k then kv else (fst kv, f (snd kv) v)


-- Ejercicio 4

groupByKey :: Eq k => [(k,v)] -> Dict k [v]
-- obtiene una lista de todos los keys en la lista de entrada removiendo duplicados
-- luego, el resultado es una lista de pares donde a cada key unica `k` le asigna un listado
-- con todos los valores que aparecen en un par asociado con `k`

groupByKey xs = [(k, valuesOf k) | k <- keys xs]
  where
    keys xs = nub $ map fst $ xs
    valuesOf k = map snd $ filter (\kv -> (fst kv) == k) $ xs


-- Ejercicio 5

unionWith :: Eq k => (v -> v -> v) -> Dict k v -> Dict k v -> Dict k v
-- devuelve una lista donde, por cada key `k` en la union de los keys
-- de los diccionarios de entrada `d1` y `d2`, se incluye un par (k, v)
-- con el valor correspondiente a `k` si este aparece en un unico diccionario,
-- o con el valor f(v1, v2) si aparece en ambos

unionWith f d1 d2 = [(k, valueOf k) | k <- union (keys d1) (keys d2)]
  where
    keys d = map fst d
    valueOf k
      | (d1 ? k) && (d2 ? k)  = f (d1 ! k) (d2 ! k)
      | (d1 ? k)              = (d1 ! k)
      | otherwise             = (d2 ! k)


-- ------------------------------Sección 2 : MapReduce-----------------------------------------

type Mapper a k v = a -> [(k,v)]
type Reducer k v b = (k, [v]) -> [b]
data Structure = Street | City | Monument deriving Show

-- Ejercicio 6

distributionProcess :: Int -> [a] -> [[a]]
-- a cada elemento x de xs lo transforma en (i, x), donde i es el
-- bucket que le corresponde. luego, la respuesta es una comprensión
-- de n buckets, cada uno de ellos con los elementos que le corresponden.

distributionProcess n xs = [bucketElements i | i <- [1..n]]
  where
    ixs = zip [1 + mod (i - 1) n | i <- [1..length xs]] xs
    bucketElements i = map snd $ filter (\ix -> (fst ix) == i) $ ixs


-- Ejercicio 7

mapperProcess :: Eq k => Mapper a k v -> [a] -> [(k,[v])]
-- aplica la funcion mp a cada elemento de xs, generando
-- muchas listas de tipo [(k, v)]. luego las concatena en
-- una unica lista y aplica la funcion groupByKey para
-- agrupar los pares (k, v) por clave

mapperProcess mp xs = groupByKey $ concat $ map mp $ xs


-- Ejercicio 8

combinerProcess :: (Eq k, Ord k) => [[(k, [v])]] -> [(k,[v])]
-- dada la lista con el resultado del mapperProcess 'ejecutado'
-- en cada maquina, une los resultados en un unico diccionario
-- donde a cada clave se le asigna la concatenacion de todos sus
-- values en todos los resultados parciales

combinerProcess xs = foldr (\x r -> unionWith (++) x r) [] $ xs


-- Ejercicio 9

reducerProcess :: Reducer k v b -> [(k, [v])] -> [b]
-- aplica la funcion reducer a cada par (k, [v]) producto del
-- proceso de combinacion, generando en cada caso una lista de
-- resultados. luego, se concatenan los resultados generados para
-- todas las claves

reducerProcess rd xs = concat $ map rd $ xs


-- Ejercicio 10

mapReduce :: (Eq k, Ord k) => Mapper a k v -> Reducer k v b -> [a] -> [b]
-- primero distribuye todos los documentos a procesar de la entrada
-- entre 100 maquinas, luego aplica el mapperProcess a cada subconjunto
-- de la entrada (simulando la ejecucion en multiples maquinas).
-- luego se combinan los resultados parciales del mapeo en cada maquina
-- y se realiza el proceso de reduccion

mapReduce mp rd xs = reduced $ combined $ mapped $ distributed $ xs
  where
    distributed = distributionProcess 100
    mapped = map $ mapperProcess mp
    combined = combinerProcess
    reduced = reducerProcess rd


-- Ejercicio 11

visitasPorMonumento :: [String] -> Dict String Int
-- cada vez que se procesa un monumento, se emite el par (nombre, 1)
-- luego la funcion de reduccion simplemente cuenta la cantidad de 1's
-- que aparecen como valor para una key determinada

visitasPorMonumento = mapReduce mp rd
  where
    mp s = [(s, 1)]
    rd (k, vs) = [(k, length vs)]


-- Ejercicio 12

monumentosTop :: [String] -> [String]
-- computa la cantidad de visitas por monumento (utilizando el ejercicio previo)
-- y luego realiza otra etapa de procesamiento mapReduce donde se ordenan
-- las tuplas (monumento, cantidad de visitas)

monumentosTop xs = mapReduce mp rd $ visitasPorMonumento $ xs
  where
    mp (s, i) = [(1, (s, i))]
    rd (k, vs) = map fst $ sortBy (\a b -> compare (snd b) (snd a)) $ vs


-- Ejercicio 13 

monumentosPorPais :: [(Structure, Dict String String)] -> [(String, Int)]
-- la funcion de mapeo solo emite key-value's en el caso de que
-- el documento procesado sea un Monumento, ignorando los demas features.
-- para cada monumento, se emite el key-value (pais, 1), permitiendo que en
-- la etapa de reduccion se cuente la cantidad de veces que aparece cada pais
-- simplemente contando la cantidad de 1's (similar al ejercicio 11)

monumentosPorPais = mapReduce mp rd
  where
    mp (Monument, dt) = [(dt ! "country", 1)]
    mp (_, dt) = []
    rd (k, vs) = [(k, length vs)]

