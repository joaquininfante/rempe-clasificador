## AUTOMATIZACIÓN DE LA FUNCIÓN DE CLASIFICACIÓN

library(tidyr)
library(dplyr)
library(bit64)
library(stringr)
library(writexl)
library(tm)
library(stringdist)

################################################################################
# CONEXIÓN A LA BASE DE DATOS
library(DBI)
library(RPostgres)

con_rempe <- dbConnect(
  Postgres(),
  dbname   = Sys.getenv("PGDBNAME1"),
  host     = Sys.getenv("PGHOST"),
  port     = Sys.getenv("PGPORT"),
  user     = Sys.getenv("PGUSER"),
  password = Sys.getenv("PGPASSWORD")
)

con_nomenclator <- dbConnect(
  Postgres(),
  dbname   = Sys.getenv("PGDBNAME2"),
  host     = Sys.getenv("PGHOST"),
  port     = Sys.getenv("PGPORT"),
  user     = Sys.getenv("PGUSER"),
  password = Sys.getenv("PGPASSWORD")
)


################################################################################
# DICCIONARIO DE MEDICAMENTOS
prescripcion <- dbGetQuery(con_rempe, "
  select  p.id , p.contenido , p.des_dosific , p.des_nomco , p.des_prese , p.lista_estupefaciente , 
  p.lista_psicotropo , p.nro_conte , p.laboratorio_comercializador_id
  from rempe.prescripcion p 
  where p.sw_comercializado = '1' 
  	and p.sw_envase_clinico = '0'
  	and p.cod_sitreg_id = '1' 
  	and p.cod_sitreg_presen_id = '1' 
  	and p.sw_uso_hospitalario = '0' 
  	and p.des_dosific <> 'NOMENCLATOR_EN_REVISION'
")
prescripcion$id = as.numeric(prescripcion$id)
prescripcion$laboratorio_comercializador_id = as.numeric(prescripcion$laboratorio_comercializador_id)


pres_laboratorios <- dbGetQuery(con_rempe, "
  select  id,laboratorio
  from rempe.pres_laboratorios
")
pres_laboratorios$id = as.numeric(pres_laboratorios$id)


# PASO 2: Unimos a la información de las descripciones de medicamentos, el 
# laboratorio al que pertenece
prescripcion = prescripcion %>% 
  left_join(pres_laboratorios, by = c("laboratorio_comercializador_id" = "id" ))

# PASO 3: Quitamos todo a la derecha del primer dígito
for (i in 1:nrow(prescripcion)) {
  if(grepl("\\d",prescripcion$des_nomco[i])){
    prescripcion$des_nomco[i] = sub("\\d.*","",prescripcion$des_nomco[i])
  }
  # cat("Medicamento ",i, " de ", nrow(prescripcion), " analizado\n")
}

# PASO 4: Convertimos a mayúsculas y quitamos tildes
prescripcion$des_nomco = toupper(prescripcion$des_nomco)
prescripcion$laboratorio = toupper(prescripcion$laboratorio)

quitar_tildes <- function(texto) {
  texto_sin_tildes <- chartr("ÁÉÍÓÚÀÈÌÒÙÜ", "AEIOUAEIOUU", texto)
  return(texto_sin_tildes)
}
prescripcion$des_nomco = quitar_tildes(prescripcion$des_nomco)
prescripcion$laboratorio = quitar_tildes(prescripcion$laboratorio)

# PASO 5: Quitamos algunos símbolos, como /, paréntesis, puntos, comas, etc.
quitar_simbolos2 = function(texto) {
  texto_limpio = gsub("/", " ", texto)
  texto_limpio = gsub("[(]", "", texto_limpio)
  texto_limpio = gsub("[)]", "", texto_limpio)
  texto_limpio = gsub("[.]", "", texto_limpio)
  texto_limpio = gsub(",", "", texto_limpio)
}
prescripcion$des_nomco = quitar_simbolos2(prescripcion$des_nomco)
prescripcion$laboratorio = quitar_simbolos2(prescripcion$laboratorio)

# PASO 6: Vamos a borrar las siguientes palabras
palabras = c("INFANTIL",
             "SEMANAL",
             "COMPRIMIDOS",
             "CREMA",
             "DE",
             "POLVO",
             "SABOR",
             "JERINGA",
             "SABOR",
             "ACIDO",
             "CAPSULAS",
             "CON",
             "COMPRIMIDOS",
             "PARA",
             "D",
             "SUSPENSION",
             "PLUS",
             "Y",
             "ACEITE",
             "ACETATO",
             "EN",
             "SPAIN",
             "DIARIO",
             "SABOR",
             "INYECTABLE",
             "RECUBIERTOS",
             "DURAS",
             "B",
             "PEDIATRICO",
             "CHUPAR",
             "RECTAL",
             "ADULTOS",
             "INFANTIL",
             "COR",
             "PELICULA",
             "PASTILLAS",
             "NIÑOS",
             "GOTAS",
             "KIT",
             "PERFUSION",
             "SIMPLE",
             "SOBRES",
             "C",
             "FUERTE",
             "MASTICABLES",
             "GEL",
             "ORALES",
             "PULVERIZACION",
             "SIMPLEX",
             "SOBRE",
             "CAFEINA",
             "APOSITO",
             "INYECTABLES",
             "JUNIOR",
             "USO",
             "E",
             "MIEL",
             "TUBO",
             "VERDE",
             "A",
             "EFG",
             "F",
             "NARANJA",
             "REFORZADO",
             "BLANDAS",
             "DEL",
             "CONCENTRADO",
             "LAB",
             "SEMESTRAL",
             "SULFATO",
             "ANARANJADO",
             "COLIRIO",
             "DOS",
             "DUO",
             "GRIPE",
             "K",
             "NOCHE",
             "POR",
             "TOS",
             "ADHESIVO",
             "ANTI",
             "FILM",
             "MIX",
             "O",
             "PRO",
             "RECTO",
             "SIN",
             "SOR",
             "TEST",
             "UÑAS",
             "VIAL",
             "ANTITUSIVO",
             "Y",
             "CUTANEA",
             "LIMON",
             "MENTA",
             "EN",
             "SOBRES",
             "SUSPENSION",
             "UNISOSIS",
             "VAGINAL",
             "VAGINALES",
             "VENTOLADO",
             "VI",
             "YODO",
             "RESFRIADO",
             "SILICONA",
             "SUPRA",
             "PROLONGADA",
             "GOMA",
             "H",
             "HOT",
             "KIDS",
             "ON",
             "ONE",
             "P",
             "FARMACEUTICA",
             "FUCA",
             "DISPERSION",
             "DRINK",
             "ESPUMA",
             "EXTRA",
             "BUCODISPERSABLES",
             "CLORHEXIDINA",
             "CHOCOLATE",
             "CR",
             "DERMO",
             "FARMA",
             "DR",
             "SPRAY",
             "TETRA",
             "CARE",
             "PURA",
             "VACUNA",
             "COMPUESTO",
             "DESCONGESTIVO",
             "HIPERTONICO",
             "IMPREGNADO",
             "LACTANTES",
             "MEDICA",
             "MUCOLITICO",
             "VIÑAS",
             "BUCAL",
             "EMULSION",
             "INCOLORO",
             "COOL",
             "ESTERIL",
             "FRESA",
             "JABONOSO",
             "LAXANTE",
             "COVID",
             "SOLUCION",
             "BISACOLIDO",
             "BISACODILO",
             "OROS",
             "ORAL",
             "LIQUIDO"
)

# Creamos el patrón con esta función a partir de las palabras
patron <- paste0("\\b(", paste(palabras, collapse = "|"), ")\\b")

for (i in 1:nrow(prescripcion)) {
  if (grepl(patron,prescripcion$des_nomco[i])) {
    prescripcion$des_nomco[i] = str_remove_all(prescripcion$des_nomco[i],patron)
  }
  # cat("Medicamento ",i, " de ", nrow(prescripcion), " analizado\n")
}


# PASO 7: Vamos a quitar las palabras del laboratorio
guardar_palabras <- function(frase) {
  palabras <- unlist(strsplit(frase, "\\s+"))  
  return(palabras)
}

for (i in 1:nrow(prescripcion)) {
  v = guardar_palabras(prescripcion$laboratorio[i])
  patron <- paste0("\\b(", paste(v, collapse = "|"), ")\\b")
  
  if (grepl(patron,prescripcion$des_nomco[i])) {
    prescripcion$des_nomco[i] = str_remove_all(prescripcion$des_nomco[i],patron)
  }
  # cat("Medicamento ",i, " de ", nrow(prescripcion), " analizado\n")
}

# PASO 8: Vamos a quitar espacios
quitar_espacios_extra <- function(texto) {
  texto_limpio <- gsub("\\s+", " ", texto)  # Reemplaza múltiples espacios con uno solo
  return(trimws(texto_limpio))  # Elimina espacios al inicio y final
}
prescripcion$des_nomco = quitar_espacios_extra(prescripcion$des_nomco)

# PASO 9: Vamos a borrar las siguientes palabras, relevantes a farmacéuticas, para mejorar la clasificación 
for (i in 1:nrow(prescripcion)) {
  patron = "STADAPHARM|FARMAMABO|RATIOMED|\\+|STADAFARMA|RATIOPHARM|CINFAMED|-RATIOPHARMA|-RATIO|- RATIO|PHARMACEUTICALS|PHARMAGENUS|PHARMAKERN|PHARMATRES"
  if(grepl(patron,prescripcion$des_nomco[i])) {
    prescripcion$des_nomco[i] = gsub(patron,"",prescripcion$des_nomco[i])
  }
  # cat("Medicamento ",i, " de ", nrow(prescripcion), " analizado\n")
}

# PASO 10: Tokenizamos la descripción de los medicamentos para ver cuales son las 
# palabras más repetidas y eliminar aquellos que puedan interferir en la clasificación
# de las prescripciones de OTROS_PRODUCTOS
repetidas = as.data.frame(unlist(strsplit(prescripcion$des_nomco, " ")))
names(repetidas) = "palabras"

df = repetidas %>% 
  select(palabras) %>% 
  count(palabras, sort = T)

for (i in 1:nrow(prescripcion)) {
  patron = "\\b(DOSIS|UNIDOSIS|EFERVESCENTES|AMPOLLA[S]?|INSTANT|RAPID|AGUA|FORTE|GRANULADO|CACAO|POMADA|JARABE|LABIAL|ANTIALERGICO|ANESTESICO|DOBLE|SEMANAL|INICIO|ACTIVADO|VEGETAL|MED|SAN PELLEGRINO|KERN|CONGESTION|MUCOSIDAD|GEN|VITAMINADO|CHOQUE|PREDENTAL|INHALACION|CLINICS|ADSORBENTE|SOLUBLE|EXPECTORANTE|OFTALMIC|UNGUENTO|PRECARGADA|ULTRA |NEO|RECTANGULAR|MEDICAMENTOSO|GASTRORRESISTENTES|COMPLEX|TRIMESTRAL|PHARMA|PHARM|FARMA|CINFA|NORMO|TEVA|QUALIGEN|RATIO|MONODOSIS|EFERVESCENTE|RETARD|OTICO|SERRA|DERMATOLOGICA|GROUP|NASAL|MENSUAL|MONODOSIS|PEREZGIMENEZ|PENSAVITAL|FLAS|SODIC|CICATRIZANTE|PROLONG|TECNIGEN|O|\\+|PENSA|NEUTRO|A|ALTER|OVULOS|TOPICA|ARRIÑONADA|S|POTASIO|MONO|MAX|BISACODILO|POS|IFC|WASH|TUSS|EMULGEL)\\b"
  if(grepl(patron,prescripcion$des_nomco[i])) {
    prescripcion$des_nomco[i] = gsub(patron,"",prescripcion$des_nomco[i])
  }
  # cat("Medicamento ",i, " de ", nrow(prescripcion), " analizado\n")
}

# PASO 11: Quitamos -
for (i in 1:nrow(prescripcion)) {
  patron = "-"
  if(grepl(patron,prescripcion$des_nomco[i])) {
    prescripcion$des_nomco[i] = gsub(patron,"",prescripcion$des_nomco[i])
  }
  # cat("Medicamento ",i, " de ", nrow(prescripcion), " analizado\n")
}

prescripcion$des_nomco = quitar_espacios_extra(prescripcion$des_nomco)

# PASO 12: Creamos el diccionario, seleccionando la variable limpia del nombre del 
# medicamento y creando una variable que sea si ese medicamente es estupefaciente o psicotropo
diccionario_med = prescripcion %>% 
  distinct(des_nomco, lista_estupefaciente, lista_psicotropo) %>% 
  mutate("estup_psicot" = !is.na(lista_estupefaciente) | !is.na(lista_psicotropo))


diccionario_med = diccionario_med %>% 
  select(des_nomco, estup_psicot)

names(diccionario_med) = c("med", "estup_psicot")

################################################################################
# DICCIONARIO LABORATORIOS DE OTROS PRODUCTOS
laboratories <- dbGetQuery(con_nomenclator, "
  select *
  from catalog.laboratories
")
laboratories$id = as.numeric(laboratories$id)
laboratories$code = as.numeric(laboratories$code)
laboratories$license = as.logical(laboratories$license)


# PASO 2: Pasamos los caracteres a mayúsculas
laboratories$name = toupper(laboratories$name)

# PASO 3: Quitamos las tildes
quitar_tildes <- function(texto) {
  texto_sin_tildes <- chartr("ÁÉÍÓÚÀÈÌÒÙÖÜ", "AEIOUAEIOUOU", texto)
  return(texto_sin_tildes)
}
laboratories$name = quitar_tildes(laboratories$name)

# PASO 4: Creamos una función que quite algunos símbolos, términos referentes a las empresas
# y a los laboratorios
limpiar_lab = function(texto) {
  texto_limpio = gsub(",", " ", texto)
  texto_limpio = gsub("[&]", " ", texto_limpio)
  texto_limpio = gsub("-|ª", " ", texto_limpio)
  texto_limpio = gsub("(S[.][^ ].*|\\bSA\\b.*|\\bSL\\b.*|\\bSAL\\b.*|\\bSLU\\b.*|\\bS\\b.*|\\bSLL\\b.*|\\bLLC\\b|\\bSRL\\b|\\bCIA\\b|\\bY\\b|\\bDE\\b|\\bBY\\b|\\bAND\\b)", "", texto_limpio)
  texto_limpio = gsub("[.]", " ", texto_limpio)
  texto_limpio = gsub("\\b(LAB|FARMA|PHARMA|LA|DEL|PRO|LABS)\\b", "", texto_limpio)
  texto_limpio = gsub("[(].*[)]", "", texto_limpio)
  texto_limpio = gsub("\\b\\S*LABORATO\\S*\\b", "", texto_limpio)
}

laboratories$name = limpiar_lab(laboratories$name)

# Cambios los 4 siguientes a mano
for (i in 1:nrow(laboratories)) {
  if(laboratories$name[i] == "I M MANUFACTURAS") {
    laboratories$name[i] = "IM MANUFACTURAS"
  }
  
  if(laboratories$name[i] == "J B J  EXCLUSIVAS  ") {
    laboratories$name[i] = "JBJ EXCLUSIVAS"
  }
  
  if(laboratories$name[i] == "B G T  INTERNATIONAL  ") {
    laboratories$name[i] = "BGT INTERNATIONAL"
  }
  
  if(laboratories$name[i] == "A C P G ") {
    laboratories$name[i] = "ACPG"
  }
}

quitar_espacios_extra <- function(texto) {
  texto_limpio <- gsub("\\s+", " ", texto)  # Reemplaza múltiples espacios con uno solo
  return(trimws(texto_limpio))  # Elimina espacios al inicio y final
}
laboratories$name = quitar_espacios_extra(laboratories$name)

# PASO 5: Eliminamos de los campos las letras que estén solas, excepto la L (por L'OREAL) 
# y la Q (por Q PHARMA)
quitar_letras = function(texto) {
  patron = "\\b(A|B|C|D|E|F|G|H|I|J|K|M|N|Ñ|O|P|R|S|T|U|V|W|X|Y|Z)\\b"
  texto_limpio = gsub(patron,"",texto)
}

laboratories$name = quitar_espacios_extra(quitar_letras(laboratories$name))

# PASO 6: Tokenizamos para ver las palabras más repetidas y que serán eliminadas
extraer_palabras <- function(frases) {
  palabras <- unlist(strsplit(frases, "\\s+"))
}

df = as.data.frame(extraer_palabras(laboratories$name))
names(df) = "pal"
df = df %>% 
  select(pal) %>% 
  count(pal)

quitar_palabras_repetidas = function(texto){
  patron = "\\b(LIMITADA|GROUP|FARMACIA|FARM|ESP|INTERNACIONAL|SPAIN|ESPAÑA|IRELAND|LIMITED|PHARMACEUTICALS|IBERICA|EUR|FARMACEUTICA|DISTRIBUCION|ESPAÑOLA|INTERNATIONAL|HOSPITAL|PHARMACEUTICAL|LOGISTICA|ITALIA|IBERIA|HISPANIA|FARMACEUTICAS|GRUPO)\\b"
  texto_limpio = gsub(patron,"",texto)
}

laboratories$name = quitar_espacios_extra(quitar_palabras_repetidas(laboratories$name))

# PASO 7: Creamos el diccionario de laboratorios con el id y el nombre
diccionario_lab = laboratories %>% 
  select(id, name)

################################################################################
products <- dbGetQuery(con_nomenclator, "
  select *
  from catalog.products
")

products$id = as.numeric(products$id)


laboratory_product <- dbGetQuery(con_nomenclator, "
  select *
  from catalog.laboratory_product
")
laboratory_product$product_id = as.numeric(laboratory_product$product_id)
laboratory_product$laboratory_id = as.numeric(laboratory_product$laboratory_id)
laboratory_product$type = as.character(laboratory_product$type)

# PASO 2: Definimos las funciones que nos serán útiles en el desarrollo
quitar_tildes <- function(texto) {
  texto_sin_tildes <- chartr("ÁÉÍÓÚÀÈÌÒÙÜ", "AEIOUAEIOUU", texto)
  return(texto_sin_tildes)
}

quitar_simbolos = function(texto) {
  texto_limpio = gsub("[*]", "", texto)
  texto_limpio = gsub("[(]", " ", texto_limpio) 
  texto_limpio = gsub("[)]", " ", texto_limpio)
  texto_limpio = gsub("®", "", texto_limpio)
  texto_limpio = gsub("[/]", " ", texto_limpio)
  texto_limpio = gsub("=", "", texto_limpio)
  texto_limpio = gsub(":", "", texto_limpio)
  texto_limpio = gsub("\\[", "", texto_limpio)
  texto_limpio = gsub("\\]", "", texto_limpio)
  texto_limpio = gsub("·","",texto_limpio)
  texto_limpio = gsub("º","",texto_limpio)
  texto_limpio = gsub("-"," ",texto_limpio)
  texto_limpio = gsub("™","",texto_limpio)
  texto_limpio = gsub("#","",texto_limpio)
  texto_limpio = gsub("ª","",texto_limpio)
  texto_limpio = gsub("\\^","",texto_limpio)
  texto_limpio = gsub("[!]","",texto_limpio)
  
  texto_limpio = gsub("^_", "", texto_limpio)
  texto_limpio = gsub("^[+]", "", texto_limpio)
  
  texto_limpio = gsub("--", "", texto_limpio)
  texto_limpio = gsub("---", "", texto_limpio)
  texto_limpio = gsub(" - | – ", " ", texto_limpio)
  
  texto_limpio = gsub("(\\d)[,|'](\\d)", "\\1\\.\\2", texto_limpio)
  texto_limpio = gsub("’","\\.",texto_limpio)
  
  texto_limpio = gsub("[.]$", "", texto_limpio)
  texto_limpio = gsub(",", "", texto_limpio)
  texto_limpio = gsub(";", "", texto_limpio)
  texto_limpio = gsub("1 000", "1000", texto_limpio)
  texto_limpio = gsub("([^0-9])[.]([^0-9])", "\\1\\2", texto_limpio)
  texto_limpio = gsub("([^0-9])[.]([0-9])", "\\1 \\2", texto_limpio)
  texto_limpio = gsub("([0-9])[.]([^0-9])", "\\1 \\2", texto_limpio)
  texto_limpio = gsub("(?<!\\d)\\.(?!\\d)", "", texto_limpio, perl = TRUE)
  texto_limpio = gsub("[.] ", " ", texto_limpio)
  
  
  
  return(texto_limpio)  
}

quitar_espacios_extra <- function(texto) {
  texto_limpio <- gsub("\\s+", " ", texto)  # Reemplaza múltiples espacios con uno solo
  return(trimws(texto_limpio))  # Elimina espacios al inicio y final
}

# PASO 3: Creamos la tabla de productos, haciendo joins entre products, laboratories y laboratory_product
productos = laboratory_product  %>%
  filter(type == "COMERCIALIZER") %>%
  left_join(laboratories, by = c("laboratory_id" = "id")) %>%
  left_join(products, by = c("product_id" = "id")) %>%
  select(product_id, laboratory_id, "laboratory_name" = name, nationalCode, denomination,presentation)

# PASO 4: Convertimos a mayúsculas el campo de presentation, quitamos tildes, símbolos y espacios innecesarios
productos$presentation = toupper(productos$presentation)
productos$presentation = quitar_tildes(productos$presentation)
productos$presentation = quitar_simbolos(productos$presentation)
productos$presentation = quitar_espacios_extra(productos$presentation)
productos$presentation = gsub("ROCHEPOSAY","ROCHE POSAY",productos$presentation)

# PASO 5: Crear un corpus y eliminar las stopwords en español
corpus <- Corpus(VectorSource(productos$presentation))
corpus <- tm_map(corpus, removeWords, quitar_tildes(toupper(stopwords("spanish"))))
# Convertir el resultado a texto limpio
productos$presentation <- sapply(corpus, as.character)

# PASO 6: Realizamos un proceso de homogeneización a la limpieza que hemos realizado
# en las prescripciones para facilitar la clasificación de estas en productos del catálogo
productos$presentation = quitar_espacios_extra(productos$presentation)
productos$presentation = gsub("([^A-Z])CAP[S]?(\\b)","\\1 CAPSULAS\\2", productos$presentation)
productos$presentation = gsub("CAPSUL(\\b)","CAPSULAS\\1", productos$presentation)
productos$presentation = gsub("COMP[R]?(\\b)","COMPRIMIDOS\\1", productos$presentation)
productos$presentation = gsub("(\\b)U(\\b)","\\1UNIDADES\\2", productos$presentation)
productos$presentation = gsub("(\\b)UD[S]?(\\b)","\\1UNIDADES\\2", productos$presentation)
productos$presentation = gsub("(\\b)UNID[S]?(\\b)","\\1UNIDADES\\2", productos$presentation)
productos$presentation = gsub("EFERV(\\b)","EFERVESCENTES\\1", productos$presentation)
productos$presentation = gsub("MG ML","MGML", productos$presentation)
productos$presentation = gsub("MILIGRAMO[S]?(\\b)","\\1MG\\2", productos$presentation)
productos$presentation = gsub("GRAMO[S]?(\\b)","\\1G\\2", productos$presentation)
productos$presentation = gsub("GR[S]?(\\b)","\\1G\\2", productos$presentation)
productos$presentation = gsub("MILIMETRO[S]?(\\b)","\\1MM\\2", productos$presentation)
productos$presentation= gsub("METRO[S]?(\\b)","\\1M\\2", productos$presentation)
productos$presentation = gsub("CENTIMETRO[S]?(\\b)","\\1CM\\2", productos$presentation)
productos$presentation = gsub("MILILITRO[S]?(\\b)","\\1ML\\2", productos$presentation)
productos$presentation = gsub("LITRO[S]?(\\b)","\\1L\\2", productos$presentation)
productos$presentation = gsub("MGR[S]?(\\b)","\\1MG\\2", productos$presentation)
productos$presentation = gsub("MILIMETRO[S]?(\\b)","\\1MM\\2", productos$presentation )
productos$presentation = gsub("MG[S]?(\\b)","\\1MG\\2", productos$presentation )
productos$presentation = gsub("MLG[S]?(\\b)","\\1MG\\2", productos$presentation )
productos$presentation = gsub("MICROGRAMO[S]?(\\b)","\\1MCG\\2",productos$presentation )
productos$presentation  = gsub("(\\b)MILILITRO[S]?(\\b)","\\1ML\\2", productos$presentation )
productos$presentation  = gsub("(\\b)LITRO[S]?(\\b)","\\1L\\2",productos$presentation )
productos$presentation  = gsub("(\\b)TAB[S]?(\\b)","\\1TABLETAS\\2",productos$presentation )
productos$presentation = gsub("(\\d+)([A-ZΜ%])","\\1 \\2", productos$presentation)
productos$presentation  = gsub("(\\d*[.]?\\d+)[ ]?(ML|L|MM||CM|M)?[ ]?X[ ]?(\\d*[.]?\\d+)([A-Z]+)?", "\\1 \\2 X \\3 \\4", productos$presentation )
productos$presentation = gsub("([^0-9]5) ([A])","\\1\\2", productos$presentation)
productos$presentation = gsub("(SPF) (\\d+)","\\1\\2", productos$presentation)
productos$presentation = gsub("([A-Z]&[^G]) (4[^0-9])","\\1\\2", productos$presentation)
productos$presentation = gsub("(\\d+)[ ]?P(\\b)","\\1 PIEZAS\\2", productos$presentation)
productos$presentation = gsub("JBE(\\b)"," JARABE\\1", productos$presentation)
productos$presentation = gsub("JB(\\b)"," JARABE\\1", productos$presentation)
productos$presentation = quitar_espacios_extra(productos$presentation)

# PASO 7: Copiamos el campo de presentation para eliminar de este posibles contenidos, volúmenes,...
productos$presentation2 = productos$presentation

# PASO 8: Creamos los campos auxiliares similares a los creados en la limpieza de prescripciones
productos$contenido = rep(NA, nrow(productos))
for (i in 1:nrow(productos)) {
  patron = "((\\d*)(\\.\\d*)?([^A-Z][ ]?X[ ]?\\d*)?(?:CAJA|PACK|BOTE|BOTE NEUTRO|TUBO|PERLA[S]?|FRASCO[S]?|TABLET[A]?[S]?)?(?: CON| DE)?\\s?\\d+\\s?(CANULAS VAGINALES|APLICADORES VAGINALES|APLICADOR VAGINAL|CANULA VAGINAL|PIEZA[S]?|PERLA[S]?|CARTUCHOS INYECTABLES|CARTUCHOS|OVULOS|PLUMA[S] PRECARGADA[S]|PLUMA[S]|SOLUCION ORAL|SOLUCION.*INYECTABLE|SOLUCION|JERINGA[S]?|CAPSULAS GELATINA BLANDA|CAPSULAS VEGETALES|CAPSULAS ORALES|MINISOBRES|VEGCAPS|AMPOLLA[S]?|SPRAY.*NASAL|SPRAY[S]?|APOSITO[S]?|BRICK[S]?|UNIDADES|FRASCO[S]?|BOTELLA[S]?|CAJA[S]?|ENVASE[S]?|COMPRIMIDO[S]? EFERVESCENTE[S]?|COMPRIMIDO[S]? .* MASTICABLE[S]?|COMPRIMIDOS MAST\\w*\\b|COMPRIMIDO[S]?|COMPR RECUBIERTOS|COMP|SOBRE[S]?|DOSI[S]?|CAPSULA[S]?|CAPSULE[S]?|CAP[S]?|VIALES BEBIBLES|VIALES|UNIDADES|BOTE[S]?|\\d{1,2}[ ]?STICK[S]?|MONODOSIS|UNIDOSIS|CAPULAS|CASPULAS|X\\s*\\d+ [^CM])\\b)|\\d*[ ]?(TUBO|TABLET[A]?S|SOLUCION.*INTECTABLE|SOLUCION CUTANEA)"
  if(grepl(patron,productos$presentation2[i])) {
    productos$contenido[i] = str_extract(productos$presentation2[i],patron)
    productos$presentation2[i] = str_remove_all(productos$presentation2[i],patron)
  }
  # cat("Producto ",i, " de ", nrow(productos), " analizado\n")
}


productos$forma = rep(NA, nrow(productos))
productos$forma2 = rep(NA, nrow(productos))

for (i in 1:nrow(productos)) {
  patron = "\\b(TUBO GEL|GEL|GEL VAGINAL|APOSITO[S]?|OVULO[S]|PLUMA[S]? PRECARGADA[S]|ENVASE[S]?|COMPRIMIT[S]?|CANULAS VAGINALES|APLICADORES VAGINALES|APLICADOR VAGINAL|CANULA VAGINAL|COMPRIMIDOS RECUBIERTOS CON PELICULA EFG|COMPRIMIDOS RECUBIERTOS CON PELICULA|CAPSULAS ORALES|CON PELICULA|COMPR|CON PELICULA EFG|EFG|COMPRIM[M]?IDO[S]?|COMPRIMIDOS EFERVESCENTES|COMPRIMIDOS MAST\\w*|COMPRIMIDOS DISPERSABLES|COMPRIMIDOS DE LIBERACION PROLONGADA EFG|COMP .*|COMP|SOBRES DE GRANULADO|SOBRES|CAPSULAS GELATINA BLANDA|CAPSULAS ORALES|CAPSULAS BLANDAS|CAPSULA[S]? DURA[S]?|CAPSUL[A]?[E]?[S]?|CAPS|PLUMA[S]?|VIALES BEBIBLES|VIALES|MONODOSIS|SPRAY|SUSPENSION INYECTABLE|SUSPENSION ORAL|SUSPENSION|CREMA|GREMA|POMADA|GOTAS ORALES|GOTAS|GOTA|CHAMPU[N]?|SHAMPOO|AGUJA|AGUJAS|SOLUCION ORAL|SOLUCION INYECTABLE|SOLUCION|SOLUCAO ORAL|PREPROBIOTICO CON ENZIMAS DIGESTIVAS|COLUTORIO|ESPUMA|JARABE|EMULSION INYECTABLE|EMULSION|TOALLITAS|ORAL)\\b"
  if(grepl(patron,productos$presentation2[i])) {
    productos$forma[i] = str_extract(productos$presentation2[i],patron)
    productos$presentation2[i] = str_remove_all(productos$presentation2[i],productos$forma[i])
  }
  if(grepl(patron,productos$presentation2[i])) {
    productos$forma2[i] = str_extract(productos$presentation2[i],patron)
    productos$presentation2[i] = str_remove_all(productos$presentation2[i],productos$forma2[i])
  }
  # cat("Producto ",i, " de ", nrow(productos), " analizado\n")
}


productos$concentracion = rep(NA, nrow(productos))
for (i in 1:nrow(productos)) {
  patron = "(\\d+\\.?\\d*\\s?%)|(\\d+\\.?\\d*\\s?(MU|MG|MILIGRAMOS|MLG|M|UI|UNIDADES))\\s+(\\d*\\.?\\d*\\s?(ML|L|MILILITROS|LITROS)\\b)"
  if(grepl(patron,productos$presentation2[i])) {
    productos$concentracion[i] = str_extract(productos$presentation2[i],patron)
    productos$presentation2[i] = str_remove_all(productos$presentation2[i],patron)
  }
  # cat("Producto ",i, " de ", nrow(productos), " analizado\n")
}

productos$masa_dimension = rep(NA, nrow(productos))
for (i in 1:nrow(productos)) {
  patron = "((\\d+[.]?\\d*[ ]?(((CM|MM|M)[ ]?X[ ]?\\d+[.]?\\d*[ ]?(CM|MM|M)\\b)|UFC|MG|MGRS|MGS|G|MICROGRAMOS|MILIGRAMO[S]?|GRAMO[S]?|GR|MCG|MLG|ΜG|M|MM|MGR|MILIGRAMS|MR)\\b)|([C]?[M]?M[ ]?\\d+[.]?\\d*[ ]?X[ ]?\\d+[.]?\\d*\\b))|(\\d+[ ]?X[ ]?\\d+[ ]?(CM|M))"
  if(grepl(patron,productos$presentation2[i])) {
    productos$masa_dimension[i] = str_extract(productos$presentation2[i],patron)
    productos$presentation2[i] = str_remove_all(productos$presentation2[i],patron)
  }
  # cat("Producto ",i, " de ", nrow(productos), " analizado\n")
}


productos$volumen = rep(NA, nrow(productos))
for (i in 1:nrow(productos)) {
  patron = "(\\d* )?(PLUMA[S]? PRECARGADA[S] DE |JERINGA[S] PRECARGADA[S]?)?(\\d{1,}[.]?\\d*[ ]?|\\d+[.]?\\d*[ ]?X[ ]?\\d+[.]?\\d*)(L|ML|LITROS|MILILITROS|UI)\\b"
  if(grepl(patron,productos$presentation2[i])) {
    productos$volumen[i] = str_extract(productos$presentation2[i],patron)
    productos$presentation2[i] = str_remove_all(productos$presentation2[i],patron)
  }
  # cat("Producto ",i, " de ", nrow(productos), " analizado\n")
}

# PASO 9: Haciendo uso del diccionario de laboratorios, vamos a tomar el id de los laboratorios que su nombre 
# aparece incrustado en alguna palabra de sus productos
productos$presentation2 = quitar_espacios_extra(productos$presentation2)
productos = productos %>% left_join(diccionario_lab, by=c("laboratory_id" = "id"))

id_lab = c()
for (i in 1:nrow(productos)) {
  if(grepl(paste0(productos$name[i],"[A-Z]"), productos$presentation[i]) | grepl(paste0("[A-Z]",productos$name[i]), productos$presentation[i])) {
    id_lab = c(id_lab, productos$laboratory_id[i])
  }
  # cat("Producto ",i, " de ", nrow(productos), " analizado\n")
}

id_lab = c(id_lab, 2000274)
id_lab = unique(id_lab)


palabras <- unique(unlist(strsplit(diccionario_lab$name, " ")))
palabras = setdiff(palabras,c("SALUD","AGUA","Q","BIO","NUTRICION","NATURAL","MAX","THE","REGULADORA","BEBE","MAR","CIRUGIA","NATUR","ENERGY", "HEALTH", "FLORADIX","CASEN","APOSITOS", "CARE"))
lab_no_eliminar = c("NORD","KERN","BAC","BARD","MABO",
                    "FAES"   , "LANIER"  ,"ARISTO",  "OIKO"  ,  "FARDI" ,  "IDEO",    
                    "SEID" ,   "ISDIN",   "NARVAL",  "CORYSAN" ,"INDAS" ,  "URGO"  , 
                    "VERKOS",  "ALTER", "VICHY", "SKINCEUTICALS", "ROCHE","POSAY","CERAVE")
patron = paste(setdiff(palabras, lab_no_eliminar), collapse = "|")
patron = paste0("(\\b(",patron,")\\b)|")

for (i in 1:nrow(productos)) {
  if(grepl(patron, productos$presentation2[i])) {
    productos$presentation2[i] = str_remove_all(productos$presentation2[i], patron)
  }
  # cat("Producto ",i, " de ", nrow(productos), " analizado\n")
}

productos$presentation2 = quitar_espacios_extra(productos$presentation2)

# PASO 10: Vamos a eliminar los nombres de laboratorio que aparezcan sueltos en los productos
productos$presentation3 =  productos$presentation2

palabras <- unique(unlist(strsplit(diccionario_lab$name, " ")))
palabras = setdiff(palabras,c("SALUD","AGUA","Q","BIO","NUTRICION","NATURAL","MAX","THE","REGULADORA","BEBE","MAR","CIRUGIA","NATUR","ENERGY", "HEALTH", "FLORADIX","CASEN","APOSITOS", "CARE"))
patron = paste(palabras,collapse = "|")
patron = paste0("(\\b(",patron,")\\b)|")

for (i in 1:nrow(productos)) {
  if(grepl(patron, productos$presentation3[i])) {
    productos$presentation3[i] = str_remove_all(productos$presentation3[i], patron)
  }
  cat("Producto ",i, " de ", nrow(productos), " analizado\n")
}

productos$presentation3 = quitar_espacios_extra(productos$presentation3)

################################################################################
# FUNCIÓN CLASIFICA
clasifica = function(prescription) {
  # PASO 1: Tomamos la hora y fecha actual del sistema para evaluar el tiempo de 
  # ejecución. Además, pedimos al programa que no muestre 
  # mensajes de warning 
  inicio = Sys.time()
  old_warn <- options(warn = -1)  # Desactiva warnings
  # # cat("CARGANDO LOS DATOS, LIBRERÍAS Y FUNCIONES NECESARIAS...\n")
  library(tidyr)
  library(dplyr)
  library(bit64)
  library(stringr)
  library(tm)
  library(writexl)
  library(stringdist)
  
  # PASO 2: Cargamos los datos necesarios para el desarrollo
  # products = read.csv("data/products.csv")
  # laboratories = read.csv("data/laboratories.csv")
  # laboratory_product = read.csv("data/laboratory_product.csv")
  # prescripcionmed = read.csv("data/prescripcionmed.csv")
  # diccionario_med = read.table("data/diccionario_med.txt", header = T)
  # diccionario_lab = read.table("data/diccionario_lab.txt", header = T)
  # productos = read.table("data/productos.txt", header = T)
  
  # PASO 3: Tomamos ahora la fecha del sistema pero en otro formato apto para 
  # añadirlo a la salida. Creamos una carpeta para almacenar los archivos de la salida
  fecha_hora <- format(inicio, "%Y-%m-%d_%H-%M-%S")
  # salida = paste0(gsub("\\..*","",nombre_archivo),"_",fecha_hora)
  # dir.create(salida)
  
  # PASO 4: Definimos las funciones que nos serán útiles en el desarrollo
  quitar_tildes <- function(texto) {
    texto_sin_tildes <- chartr("ÁÉÍÓÚÀÈÌÒÙÜÖ", "AEIOUAEIOUUO", texto)
    return(texto_sin_tildes)
  }
  
  quitar_simbolos = function(texto) {
    texto_limpio = gsub("[*]", "", texto)
    texto_limpio = gsub("[(]", " ", texto_limpio) 
    texto_limpio = gsub("[)]", " ", texto_limpio)
    texto_limpio = gsub("®", "", texto_limpio)
    texto_limpio = gsub("[/]", " ", texto_limpio)
    texto_limpio = gsub("=", "", texto_limpio)
    texto_limpio = gsub(":", "", texto_limpio)
    texto_limpio = gsub("\\[", "", texto_limpio)
    texto_limpio = gsub("\\]", "", texto_limpio)
    texto_limpio = gsub("·","",texto_limpio)
    texto_limpio = gsub("º","",texto_limpio)
    texto_limpio = gsub("-"," ",texto_limpio)
    texto_limpio = gsub("™","",texto_limpio)
    texto_limpio = gsub("#","",texto_limpio)
    texto_limpio = gsub("ª","",texto_limpio)
    texto_limpio = gsub("\\^","",texto_limpio)
    texto_limpio = gsub("[!]","",texto_limpio)
    
    texto_limpio = gsub("^_", "", texto_limpio)
    texto_limpio = gsub("^[+]", "", texto_limpio)
    
    texto_limpio = gsub("--", "", texto_limpio)
    texto_limpio = gsub("---", "", texto_limpio)
    texto_limpio = gsub(" - | – ", " ", texto_limpio)
    
    texto_limpio = gsub("(\\d)[,|'](\\d)", "\\1\\.\\2", texto_limpio)
    texto_limpio = gsub("’","\\.",texto_limpio)
    
    texto_limpio = gsub("[.]$", "", texto_limpio)
    texto_limpio = gsub(",", "", texto_limpio)
    texto_limpio = gsub(";", "", texto_limpio)
    texto_limpio = gsub("1 000", "1000", texto_limpio)
    texto_limpio = gsub("([^0-9])[.]([^0-9])", "\\1\\2", texto_limpio)
    texto_limpio = gsub("([^0-9])[.]([0-9])", "\\1 \\2", texto_limpio)
    texto_limpio = gsub("([0-9])[.]([^0-9])", "\\1 \\2", texto_limpio)
    texto_limpio = gsub("(?<!\\d)\\.(?!\\d)", "", texto_limpio, perl = TRUE)
    texto_limpio = gsub("[.] ", " ", texto_limpio)
    
    
    
    return(texto_limpio)  
  }
  
  quitar_espacios_extra <- function(texto) {
    texto_limpio <- gsub("\\s+", " ", texto)  # Reemplaza múltiples espacios con uno solo
    return(trimws(texto_limpio))  # Elimina espacios al inicio y final
  }
  
  # PASO 5: Convertimos las variables de nuestros datos que se hayan leído como caracter
  # en lógicas (en el caso de que solo contengan true/false)
  variables = names(prescription)
  num_pres = nrow(prescription)
  
  for (i in 1:length(variables)) {
    variable = variables[i]
    valores_unicos <- unique(prescription[[variable]])
    if (all(valores_unicos %in% c("true", "false"))) {
      prescription[[variable]] <- prescription[[variable]] == "true"
    }
    # cat("Variable ",i, "de ", length(variables), " analizada. BUCLE 1 DE 46\n")
  }
  
  # PASO 6: Filtramos los datos para obtener las prescripciones de OTROS_PRODUCTOS, 
  # en los que status sea TRUE y prescription_status no sea NUEVA
  datos_op = prescription %>% 
    filter(prescription_type == "OTROS_PRODUCTOS")# %>%  
    # filter(status==TRUE & prescription_status != "NUEVA")
  num_op = nrow(datos_op)

  if(num_op > 0) {
  # PASO 7: Convertimos los valores vacíos en NA 
  # cat("CONVIRTIENDO LOS VALORES VACÍOS EN NA...\n")
  datos_op[] <- lapply(datos_op, function(x) {
    if (is.character(x) | is.factor(x)) {
      x[x == ""] <- NA
    }
    return(x)
  })
  
  ################################################################################  
  # cat("COMIENZA EL PROCESO DE LIMPIEZA\n")
  # Sys.sleep(2)
  
  # PASO 2 LIMPIEZA: Nos quedamos con las prescripciones recetadas por texto libre. 
  # Seleccionamos solo las que sean distintas para mejorar la eficiencia y creamos
  # otro campo con el nombre de la prescripción original
  datos_nocat = datos_op %>% 
    filter(is.na(presentation_type))
  num_nocat = nrow(datos_nocat)
  num_cat = num_op-num_nocat
  
  if (num_nocat > 0) {
  od = datos_nocat %>% distinct(others_description)
  od = od %>% mutate("od_original" = od$others_description)
  
  
  # PASO 3 LIMPIEZA: Convertimos los valores a mayúsculas y eliminamos las tildes
  od$others_description = toupper(od$others_description)
  od$others_description = quitar_tildes(od$others_description)
  
  
  # PASO 4 LIMPIEZA: Quitamos los símbolos
  od$others_description = quitar_simbolos(od$others_description)
  od$others_description = quitar_espacios_extra(od$others_description)
  
  # PASO 6 LIMPIEZA: Vamos a crear un campo adicional que recoja aquellas prescripciones que detectemos que no están en español o catalán 
  od$otro_idioma = rep(F, nrow(od))
  v = which(grepl("[^A-Z0-9 \\.%\\+ÑΜΑ]|\\b(DAY|COMPANY|SOLUTION|FOSFOMYCIN|SASZETKA|PELLICULE|AZITHROMYCIN|FILMTABLETTE|TABLETTE|TABLETTEN|RIGIDE|PILLS|ACIDE|TABLET[S]|SOLUZIONE|INALAZIONE|ET|DU|COMPRESSE|COMPRIME[S]?|HARD|UND|ZUR|TILL|POUR|TAKE|DISSOLVE|AND|FUR|PAR)\\b",od$others_description))
  if(length(v > 0)) {
    od$otro_idioma[v] = T
  }
  
  # PASO 7 LIMPIEZA: Vamos a crear un campo adicional para aquellas prescripciones que sean vacunas
  od$vacuna_fmagistral = rep(F, nrow(od))
  v = which(grepl("VACUNA",od$others_description))
  if(length(v) > 0){
    od$vacuna_fmagistral[v] = T 
  }
  
  
  # PASO 8 LIMPIEZA: Vamos a añadir al campo anterior aquellas prescripciones que sean fórmulas magistrales 
  v = which(grepl("\\bFORMULA MAGISTRAL\\b",od$others_description))
  if(length(v) > 0) {
    od$vacuna_fmagistral[v] = T 
  }
  v = which(grepl("%.*%",od$others_description))
  if(length(v) > 0) {
    od$vacuna_fmagistral[v] = T 
  }
  v = which(grepl("MG .* MG",od$others_description) & !grepl("\\d+[.]?\\d*[ ]?MG[ ]?\\d+[.]?\\d*[ ]?MG",od$others_description))
  if(length(v) > 0) {
    od$vacuna_fmagistral[v] = T 
  }
  
  # PASO 9 LIMPIEZA: Para las prescripciones que contengan un símbolo de + entre espacios (posible prescripción múltiple),
  # nos quedamos con los elementos antes del +.
  # Para las demás que detectamos con más de un producto, las añadimos al campo lógico mult_productos
  od$others_description =  gsub(" \\+ .*","",od$others_description)
  
  od$mult_productos = rep(F, nrow(od))
  v = which(grepl("MG\\+",od$others_description))
  if(length(v) > 0) {
    od$mult_productos[v] = T 
  }
  v = which(grepl("\\b1 .* 2\\b",od$others_description) & !grepl("\\b1 [X] 2\\b",od$others_description))
  if(length(v) > 0) {
    od$mult_productos[v] = T 
  }
  
  rownames(od) = 1:nrow(od)
  
  # PASO 10 LIMPIEZA: Vamos a crear un campo adicional en od que sea el valor de CN en el caso de que 
  # dentro de others_description hayan añadido CN y eliminamos esa parte de others_description
  od$CN = rep(NA,nrow(od))
  for (i in 1:nrow(od)) {
    patron = 
      if(grepl("[^0-9]+(\\d{5,7}\\.\\d{1,1}|\\d{6,7})\\b",od$others_description[i])) {
        od$CN[i] = str_extract(od$others_description[i],"(\\d{5,7}\\.\\d{1,1}|\\d{6,7})")
        if(nchar(od$CN[i]) == 6 | nchar(od$CN[i]) == 7 | nchar(od$CN[i]) == 8) {
          od$others_description[i] = str_remove_all(od$others_description[i],od$CN[i])
        }
        else {
          od$CN[i] = NA
        }
      }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 2 DE 46\n")
  }
  
  od$others_description = quitar_espacios_extra(od$others_description)
  
  
  # PASO 11 LIMPIEZA: Vamos a borrar los elementos HTTP, relativos a los códigos y algunas palabras
  # que hemos visto que podrían ser perjudiciales para la clasificación
  limpiar_od = function(texto) {
    texto_limpio = gsub("HTTP.*\\b","",texto)
    texto_limpio = gsub("\\b(CNF|CICN|CODIGO|NACIONAL|CODI|IDENTIFICADOR|REFERENCE|REFERENCIA|REF|CODIGO\\d{1,})\\b","",texto_limpio)
    texto_limpio = gsub("(\\b)CN ([^B])","\\1 \\2",texto_limpio)
    texto_limpio = gsub("CN$","",texto_limpio)
    texto_limpio = gsub("\\b(DURAS|BEUTEL|SAQ|VELAMOX|GRANULADO|DISPERSIVEL|MED|O SIMILAR|OPCIONES DE CHAMPU DE USO FRECUENTE|TOPICA|RETARD|LOCION|NIÑOS|TOPICO|FIBRA|HARTKAPSEL|SIMETICONA|ENV|COMBIX|LOSANGES|FILMTBL|CREME|CANTIDAD|GELULE|SACHET|ULTRA|INFANTIL|BABY|NASAL|COMPACTO|ACTO|FILTRO)\\b","",texto_limpio)
  }
  
  od$others_description = limpiar_od(od$others_description)
  
  od$others_description = quitar_espacios_extra(od$others_description)
  
  # PASO 12 LIMPIEZA: Con el diccionario de laboratorios creado, vamos a eliminar palabras
  # referentes a laboratorios del campo de prescripciones (excepto algunas que son genéricas y pueden corresponderse a productos)
  # Estas palabras las añadimos al campo de palabras_lab y el id del laboratorio lo añadimos en lab_id
  palabras <- unique(unlist(strsplit(diccionario_lab$name, " ")))
  palabras = setdiff(palabras,c("SALUD","AGUA","Q","BIO","NUTRICION","NATURAL","MAX","THE","REGULADORA","BEBE","MAR","CIRUGIA","NATUR","ENERGY", "HEALTH", "FLORADIX","CASEN","APOSITOS", "CARE", "SOLUTIONS", "ALGO"))
  lab_no_eliminar = c("NORD","KERN","BARD","MABO",
                      "FAES"   , "LANIER"  ,"ARISTO",  "OIKO"  ,  "FARDI",    
                      "SEID" ,   "ISDIN",   "NARVAL",  "CORYSAN" ,"INDAS" ,  "URGO"  , 
                      "VERKOS", "VICHY", "SKINCEUTICALS", "ROCHE","POSAY","CERAVE")
  
  patron = paste(palabras, collapse = "|")
  patron = paste0("(\\b(",patron,")\\b)|")
  patron = paste(patron,paste(lab_no_eliminar, collapse = "|"), sep = "")
  
  od$palabras_lab = rep(NA,nrow(od))
  od$lab_id = rep(NA,nrow(od))
  
  lab_no_eliminar = paste(lab_no_eliminar, collapse = "|")
  
  for (i in 1:nrow(od)) {
    if(grepl(patron,od$others_description[i])) {
      od$palabras_lab[i] = paste(str_extract_all(od$others_description[i],patron)[[1]], collapse = " ")
      if(!grepl(lab_no_eliminar,od$others_description[i])) {
        od$others_description[i] = str_remove_all(od$others_description[i],patron)
      }
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 3 DE 46\n")
  }
  
  od$others_description = gsub("LABORATORIO.*|EG LABO","",od$others_description)
  
  # PASO 13 LIMPIEZA: Si detectamos que alguna de las prescripciones contiene palabras de laboratorios
  # distintos es probable que se trate de una prescripción con más de un producto. En este caso, las vamos a eliminar
  l <- vector("list", nrow(od))
  for (i in 1:nrow(od)) {
    if(!is.na(od$palabras_lab[i])) {
      pres = unlist(strsplit(od$palabras_lab[i], " "))
      lab = strsplit(diccionario_lab$name, " ")
      for (j in 1:length(pres)) {
        for (k in 1:length(lab)) {
          if(pres[j] %in% lab[[k]]) {
            od$lab_id[i] = diccionario_lab$id[k]
            l[[i]] <- c(l[[i]],diccionario_lab$id[k])
          }
        }
      }
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 4 DE 46\n")
  }
  
  mas_un_lab = c()
  for (i in 1:nrow(od)) {
    if(length(unique(l[[i]])) > 1 && !setequal(l[[i]], c(2000279, 2000200)) 
       && !setequal(l[[i]], c(424, 2000032)) 
       && !setequal(l[[i]], c(8001, 2000059))) {
      mas_un_lab[i] = i
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 5 DE 46\n")
  }
  
  if (length(which(!is.na(mas_un_lab))) > 0) {
    od$mult_productos[which(!is.na(mas_un_lab))] = T
  }
  
  # PASO 14 LIMPIEZA: Como el laboratorio L'Óreal contiene Vichy, Skinceuticals, Cerave y Roche Posay,
  # pero en la tabla laboratory_product están todos asociados a L'Óreal, recodificamos el código
  # de estos laboratorios para que tengan el de L'Óreal (2000274)
  for (i in 1:nrow(od)) {
    if(od$lab_id[i] %in% c(2000278,2000275,2000277,2000276)) {
      od$lab_id[i] = 2000274
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 6 DE 46\n")
  }
  
  od$others_description = quitar_espacios_extra(od$others_description)
  
  # PASO 15 LIMPIEZA: Quitamos las palabras repetidas del campo de palabras_lab
  for (i in 1:nrow(od)) {
    if(!is.na(od$palabras_lab[i])) {
      od$palabras_lab[i] = paste(unique(unlist(strsplit(od$palabras_lab[i], " "))), collapse = " ")
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 7 DE 46\n")
  }
  
  
  # PASO 16 LIMPIEZA: Vamos a construir una función que tome las indicaciones que estén en el campo others_description
  # y las lleve al campo indicaciones, borrándolas del primero
  od$indicaciones = rep(NA, nrow(od))
  for (i in 1:nrow(od)) {
    patron = "(\\b(TAKE|DISSOLVE|CONCENTRADO|POR|SI NOTA|EL PACIENTE|EJERCICIO|EJERCICIOS|HACER|ALTERNAR|POLVO PARA|CHUPAR|DISPENSACION|CONSERVAR|ESPACIAR|UN|UNA|1 PERLA|COMPLEMENTO|SE|APLICAR|TOMAR|DISOLVER|INICIAR|MANTENER|1 CAP|1 CAPS|1 CAPSULA|1 CAPSULAS|CON LA|X LA MAÑANA|3 CAPSULAS|3 CAPS|2 CAPSULAS|2 CAPS|1 COMP|1 COMPRIMIDO|2 COMP|2 COMPRIMIDOS|CADA)\\b.*)|(1 COMP POR\\b.*)|(\\d+[ ]?X[ ]?DIA[S]?.*)|(\\d*[ ]?X[ ]?\\d+[ ]?(?:DIA[S]?)? [^CMS])"
    if(grepl(patron,od$others_description[i])) {
      od$indicaciones[i] = str_extract(od$others_description[i],patron)
      od$others_description[i] = str_remove_all(od$others_description[i],patron)
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 8 DE 46\n")
  }
  
  # PASO 17 LIMPIEZA: Vamos a borrar todo a la derecha de O 
  for (i in 1:nrow(od)) {
    if(grepl(" O .*|FUERZA.*",od$others_description[i])) {
      od$others_description[i] = gsub(" O .*"," O", od$others_description[i])
      od$others_description[i] = gsub("FUERZA.*","", od$others_description[i])
      od$others_description[i] = gsub(" NO .*","", od$others_description[i])
      
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 9 DE 46\n")
  }
  
  
  # PASO 18 LIMPIEZA: Si detecta TIPO se quede con la primera palabra posterior
  od$others_description = gsub(".*TIPO\\s+(\\w+).*", "\\1", od$others_description)
  
  # PASO 19 LIMPIEZA: Borramos O Y A LA DERECHA DE COMO
  for (i in 1:nrow(od)) {
    if(grepl("\\b(O|COMO)\\b",od$others_description[i])) {
      od$others_description[i] = gsub("\\b(O|COMO)\\b","", od$others_description[i])
    }
    if(grepl(" COMO .*",od$others_description[i])) {
      od$others_description[i] = gsub(" COMO .*","", od$others_description[i])
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 10 DE 46\n")
  }
  
  # PASO 20 LIMPIEZA: Creamos dos campos que recojan el posible contenido del producto
  od$contenido = rep(NA, nrow(od))
  od$contenido2 = rep(NA, nrow(od))
  for (i in 1:nrow(od)) {
    patron = "((\\d*)(\\.\\d*)?([^A-Z][ ]?X[ ]?\\d*)?(?:CAJA|PACK|BOTE|BOTE NEUTRO|TUBO|PERLA[S]?|FRASCO[S]?|TABLET[A]?[S]?)?(?: CON| DE)?\\s?\\d+\\s?(PIEZA[S]?|PERLA[S]?|CARTUCHOS INYECTABLES|CARTUCHOS|OVULOS|PLUMA[S] PRECARGADA[S]|PLUMA[S]|CANULAS VAGINALES|APLICADORES VAGINALES|APLICADOR VAGINAL|CANULA VAGINAL|SOLUCION ORAL|SOLUCION.*INYECTABLE|SOLUCION|JERINGA[S]?|CAPSULAS GELATINA BLANDA|CAPSULAS VEGETALES|CAPSULAS ORALES|MINISOBRES|VEGCAPS|AMPOLLA[S]?|SPRAY.*NASAL|SPRAY[S]?|APOSITO[S]?|BRICK[S]?|UNIDADES|FRASCO[S]?|BOTELLA[S]?|CAJA[S]?|ENVASE[S]?|COMPRIMIDO[S]? EFERVESCENTE[S]?|COMPRIMIDO[S]? .* MASTICABLE[S]?|COMPRIMIDOS MAST\\w*\\b|COMPRIMIDO[S]?|COMPR RECUBIERTOS|COMP|SOBRE[S]?|DOSI[S]?|CAPSULA[S]?|CAPSULE[S]?|CAP[S]?|VIALES BEBIBLES|VIALES|U|UDS|BOTE[S]?|\\d{1,2}[ ]?STICK[S]?|MONODOSIS|UNIDOSIS|CAPULAS|CASPULAS|X\\s*\\d+ [^CM])\\b)|\\d*[ ]?(TUBO|TABLET[A]?S|SOLUCION.*INTECTABLE|SOLUCION CUTANEA)"
    if(grepl(patron,od$others_description[i])) {
      od$contenido[i] = str_extract(od$others_description[i],patron)
      od$others_description[i] = str_remove_all(od$others_description[i],patron)
    }
    if(grepl(patron,od$others_description[i])) {
      od$contenido2[i] = str_extract(od$others_description[i],patron)
      od$others_description[i] = str_remove_all(od$others_description[i],patron)
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 11 DE 46\n")
  }
  
  
  # PASO 21 LIMPIEZA: Creamos dos campos que recojan la posible forma farmacéutica del producto
  od$forma = rep(NA, nrow(od))
  od$forma2 = rep(NA, nrow(od))
  for (i in 1:nrow(od)) {
    patron = "\\b(TUBO GEL|GEL|GEL VAGINAL|APOSITO[S]?|OVULO[S]|PLUMA[S]? PRECARGADA[S]|ENVASE[S]?|CANULAS VAGINALES|APLICADORES VAGINALES|APLICADOR VAGINAL|CANULA VAGINAL|COMPRIMIT[S]?|COMPRIMIDOS RECUBIERTOS CON PELICULA EFG|COMPRIMIDOS MAST\\w*|COMPRIMIDOS RECUBIERTOS CON PELICULA|CAPSULAS GELATINA BLANDA|CAPSULAS VEGETALES|CAPSULAS ORALES|CON PELICULA|COMPR|CON PELICULA EFG|EFG|COMPRIM[M]?IDO[S]?|COMPRIMIDOS EFERVESCENTES|COMPRIMIDOS DISPERSABLES|COMPRIMIDOS DE LIBERACION PROLONGADA EFG|COMP .*|COMP|SOBRES DE GRANULADO|SOBRES|CAPSULAS ORALES|CAPSULAS BLANDAS|CAPSULA[S]? DURA[S]?|CAPSUL[A]?[E]?[S]?|CAPS|PLUMA[S]?|VIALES BEBIBLES|VIALES|MONODOSIS|SPRAY|SUSPENSION INYECTABLE|SUSPENSION ORAL|SUSPENSION|CREMA|GREMA|POMADA|GOTAS ORALES|GOTAS|GOTA|CHAMPU[N]?|SHAMPOO|AGUJA|AGUJAS|SOLUCION ORAL|SOLUCION INYECTABLE|SOLUCION|SOLUCAO ORAL|PREPROBIOTICO CON ENZIMAS DIGESTIVAS|COLUTORIO|ESPUMA|JARABE|EMULSION INYECTABLE|EMULSION|TOALLITAS|ORAL)\\b"
    if(grepl(patron,od$others_description[i])) {
      od$forma[i] = str_extract(od$others_description[i],patron)
      od$others_description[i] = str_remove_all(od$others_description[i],od$forma[i])
    }
    if(grepl(patron,od$others_description[i])) {
      od$forma2[i] = str_extract(od$others_description[i],patron)
      od$others_description[i] = str_remove_all(od$others_description[i],od$forma2[i])
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 12 DE 46\n")
  }
  
  
  # PASO 22 LIMPIEZA: Creamos un campo que recoja la concentración
  od$concentracion = rep(NA, nrow(od))
  for (i in 1:nrow(od)) {
    # patron = "(\\d+\\.?\\d*\\s?%)|(\\d+\\.?\\d*\\s?(MU|MG|MILIGRAMOS|MLG|M)\\s?\\d*\\.?\\d*\\s?(ML|L|MILILITROS|LITROS)\\b)"
    patron = "(\\d+\\.?\\d*\\s?%)|(\\d+\\.?\\d*\\s?(MU|MG|MILIGRAMOS|MLG|M|UI|U))\\s+(\\d*\\.?\\d*\\s?(ML|L|MILILITROS|LITROS)\\b)"
    if(grepl(patron,od$others_description[i])) {
      od$concentracion[i] = str_extract(od$others_description[i],patron)
      od$others_description[i] = str_remove_all(od$others_description[i],patron)
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 13 DE 46\n")
  }
  
  # PASO 23 LIMPIEZA: Creamos dos campos que recojan la posible dimensión o 
  od$masa_dimension = rep(NA, nrow(od))
  od$masa_dimension2= rep(NA, nrow(od))
  for (i in 1:nrow(od)) {
    patron = "((\\d+[.]?\\d*[ ]?(((CM|MM|M)[ ]?X[ ]?\\d+[.]?\\d*[ ]?(CM|MM|M)\\b)|UFC|MG|MGRS|MGS|G|MICROGRAMOS|MILIGRAMO[S]?|GRAMO[S]?|GR|MCG|MLG|ΜG|M|MM|MGR|MILIGRAMS|MR)\\b)|([C]?[M]?M[ ]?\\d+[.]?\\d*[ ]?X[ ]?\\d+[.]?\\d*\\b))|(\\d+[ ]?X[ ]?\\d+[ ]?(CM|M))"
    if(grepl(patron,od$others_description[i])) {
      od$masa_dimension[i] = str_extract(od$others_description[i],patron)
      od$others_description[i] = str_remove_all(od$others_description[i],patron)
    }
    if(grepl(patron,od$others_description[i])) {
      od$masa_dimension2[i] = str_extract(od$others_description[i],patron)
      od$others_description[i] = str_remove_all(od$others_description[i],patron)
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 14 DE 46\n")
  }
  
  
  # PASO 24 LIMPIEZA: Creamos un campo que recoja el posible volumen del producto
  od$volumen = rep(NA, nrow(od))
  for (i in 1:nrow(od)) {
    patron = "(\\d* )?(PLUMA[S]? PRECARGADA[S] DE |JERINGA[S] PRECARGADA[S]?)?(\\d{1,}[.]?\\d*[ ]?|\\d+[.]?\\d*[ ]?X[ ]?\\d+[.]?\\d*)(L|ML|LITROS|MILILITROS|UI)\\b"
    if(grepl(patron,od$others_description[i])) {
      od$volumen[i] = str_extract(od$others_description[i],patron)
      od$others_description[i] = str_remove_all(od$others_description[i],patron)
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 15 DE 46\n")
  }
  
  
  # PASO 25 LIMPIEZA: Vamos a eliminar todos los números o palabras que contengan números, exceptuando los casos de 365, 50+ y 5 y de vitaminas
  for (i in 1:nrow(od)) {
    if (grepl("\\b(365|\\d{2,2}[+]|\\d{2,2} [+])| 5 | 120 |([A-Z]| )4\\b|B12|D3|K1|K2|MK7|B125|B11|B6|B3", od$others_description[i]) == F) {
      patron = "\\d+|\\d*\\.\\d*"
      if (grepl(patron, od$others_description[i])) {
        od$others_description[i] <- str_remove_all(od$others_description[i], patron)
      }  
    } 
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 16 DE 46\n")
  }
  
  od$others_description = quitar_espacios_extra(od$others_description)
  od$others_description = quitar_simbolos(od$others_description)
  od$others_description = quitar_espacios_extra(od$others_description)
  
  # PASO 26 LIMPIEZA: Homogeneizamos algunos términos
  od$others_description = gsub("([^0-9]5) ([A-Z])","\\1\\2",od$others_description)
  od$others_description = gsub("(SPF) (\\d+)","\\1\\2", od$others_description)
  od$others_description = gsub("([A-Z]) (4([^0-9]|\\b))","\\1\\2", od$others_description)
  
  
  # PASO 27 LIMPIEZA: Elimina las stopwords en español de todos los campos
  
  # Crear un corpus y eliminar las stopwords en español
  corpus <- Corpus(VectorSource(od$others_description))
  corpus <- tm_map(corpus, removeWords, quitar_tildes(toupper(stopwords("spanish"))))
  
  # Convertir el resultado a texto limpio
  od$others_description <- sapply(corpus, as.character)
  od$others_description = quitar_espacios_extra(od$others_description)
  
  # Cambiamos el nombre del primer campo
  names(od)[1] = "od_limpia"
  
  
  ## Eliminamos las stopwords en español de todos los campos
  # Crear un corpus y eliminar las stopwords en español
  corpus <- Corpus(VectorSource(od$contenido))
  corpus <- tm_map(corpus, removeWords, quitar_tildes(toupper(stopwords("spanish"))))
  
  # Convertir el resultado a texto limpio
  od$contenido <- sapply(corpus, as.character)
  od$contenido = quitar_espacios_extra(od$contenido)
  
  
  # Crear un corpus y eliminar las stopwords en español
  corpus <- Corpus(VectorSource(od$contenido2))
  corpus <- tm_map(corpus, removeWords, quitar_tildes(toupper(stopwords("spanish"))))
  
  # Convertir el resultado a texto limpio
  od$contenido2 <- sapply(corpus, as.character)
  od$contenido2 = quitar_espacios_extra(od$contenido2)
  
  corpus <- Corpus(VectorSource(od$forma))
  corpus <- tm_map(corpus, removeWords, quitar_tildes(toupper(stopwords("spanish"))))
  
  # Convertir el resultado a texto limpio
  od$forma <- sapply(corpus, as.character)
  od$forma = quitar_espacios_extra(od$forma)
  
  corpus <- Corpus(VectorSource(od$forma2))
  corpus <- tm_map(corpus, removeWords, quitar_tildes(toupper(stopwords("spanish"))))
  
  # Convertir el resultado a texto limpio
  od$forma2 <- sapply(corpus, as.character)
  od$forma2 = quitar_espacios_extra(od$forma2)
  
  corpus <- Corpus(VectorSource(od$concentracion))
  corpus <- tm_map(corpus, removeWords, quitar_tildes(toupper(stopwords("spanish"))))
  
  # Convertir el resultado a texto limpio
  od$concentracion <- sapply(corpus, as.character)
  od$concentracion = quitar_espacios_extra(od$concentracion)
  
  
  corpus <- Corpus(VectorSource(od$masa_dimension))
  corpus <- tm_map(corpus, removeWords, quitar_tildes(toupper(stopwords("spanish"))))
  
  
  # Convertir el resultado a texto limpio
  od$masa_dimension <- sapply(corpus, as.character)
  od$masa_dimension = quitar_espacios_extra(od$masa_dimension)
  
  
  corpus <- Corpus(VectorSource(od$masa_dimension2))
  corpus <- tm_map(corpus, removeWords, quitar_tildes(toupper(stopwords("spanish"))))
  
  # Convertir el resultado a texto limpio
  od$masa_dimension2 <- sapply(corpus, as.character)
  od$masa_dimension2 = quitar_espacios_extra(od$masa_dimension2)
  
  
  corpus <- Corpus(VectorSource(od$volumen))
  corpus <- tm_map(corpus, removeWords, quitar_tildes(toupper(stopwords("spanish"))))
  
  # Convertir el resultado a texto limpio
  od$volumen <- sapply(corpus, as.character)
  od$volumen = quitar_espacios_extra(od$volumen)
  
  # PASO 28 LIMPIEZA: Homogeneizamos todos los valores de los campos auxiliares para facilitar la clasificación
  od$contenido = gsub("CAP[S]?(\\b)","CAPSULAS\\1", od$contenido)
  od$contenido = gsub("COMP[R]?(\\b)","COMPRIMIDOS\\1", od$contenido)
  od$contenido = gsub("(\\b)U(\\b)","\\1UNIDADES\\2", od$contenido)
  od$contenido = gsub("(\\b)UD[S]?(\\b)","\\1UNIDADES\\2", od$contenido)
  od$contenido = gsub("(\\b)UNID[S]?(\\b)","\\1UNIDADES\\2", od$contenido)
  od$contenido = gsub("(\\d+)([A-Z])","\\1 \\2", od$contenido)
  od$contenido = gsub("(\\d?)[ ]?X[ ]?(\\d+)","\\1 X \\2", od$contenido)
  od$contenido  = gsub("(\\b)TAB[S]?(\\b)","\\1TABLETAS\\2",od$contenido)
  od$contenido = quitar_espacios_extra(od$contenido)
  
  
  od$contenido2 = gsub("CAP[S]?(\\b)","CAPSULAS\\1", od$contenido2)
  od$contenido2 = gsub("COMP[R]?(\\b)","COMPRIMIDOS\\1", od$contenido2)
  od$contenido2 = gsub("(\\b)U(\\b)","\\1UNIDADES\\2", od$contenido2)
  od$contenido2 = gsub("(\\b)UD[S]?(\\b)","\\1UNIDADES\\2", od$contenido2)
  od$contenido2 = gsub("(\\b)UNID[S]?(\\b)","\\1UNIDADES\\2", od$contenido2)
  od$contenido2 = gsub("(\\d+)([A-Z])","\\1 \\2", od$contenido2)
  od$contenido2 = gsub("(\\d?)[ ]?X[ ]?(\\d+)","\\1 X \\2", od$contenido2)
  od$contenido2  = gsub("(\\b)TAB[S]?(\\b)","\\1TABLETAS\\2",od$contenido2)
  od$contenido2 = quitar_espacios_extra(od$contenido2)
  
  
  od$forma = gsub("CAP[S]?(\\b)","CAPSULAS\\1", od$forma)
  od$forma = gsub("COMP[R]?(\\b)","COMPRIMIDOS\\1", od$forma)
  od$forma = gsub("EFERV(\\b)","EFERVESCENTES\\1", od$forma)
  od$forma = gsub("\\bEFER\\w*","COMPRIMIDOS", od$forma)
  od$forma = gsub("(\\d+)([A-Z])","\\1 \\2", od$forma)
  od$forma  = gsub("(\\b)TAB[S]?(\\b)","\\1TABLETAS\\2",od$forma)
  od$forma = quitar_espacios_extra(od$forma)
  
  
  od$forma2 = gsub("\\bCAP[S]?\\w*","CAPSULAS", od$forma2)
  od$forma2 = gsub("\\bCOMP[R]?\\w*","COMPRIMIDOS", od$forma2)
  od$forma2 = gsub("(\\d+)([A-Z])","\\1 \\2", od$forma2)
  od$forma2  = gsub("(\\b)TAB[S]?(\\b)","\\1TABLETAS\\2",od$forma2)
  od$forma2 = quitar_espacios_extra(od$forma2)
  
  
  od$concentracion = gsub("(\\d+)([A-Z%])","\\1 \\2", od$concentracion)
  od$concentracion = gsub("MG ML","MGML", od$concentracion)
  od$concentracion = gsub("UI ML","UIML", od$concentracion)
  od$concentracion = quitar_espacios_extra(od$concentracion)
  
  
  
  od$masa_dimension = gsub("(\\b)MILIGRAMO[S]?(\\b)","\\1MG\\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\b)MILIGRAM[S]?(\\b)","\\1MG\\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\b)GRAMO[S]?(\\b)","\\1G\\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\b)GR[S]?(\\b)","\\1G\\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\b)MILIMETRO[S]?(\\b)","\\1MM\\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\b)METRO[S]?(\\b)","\\1M\\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\b)CENTIMETRO[S]?(\\b)","\\1CM\\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\b)MILILITRO[S]?(\\b)","\\1ML\\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\b)LITRO[S]?(\\b)","\\1L\\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\b)MGR[S]?(\\b)","\\1MG\\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\b)MILIMETRO[S]?(\\b)","\\1MM\\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\b)MG[S]?(\\b)","\\1MG\\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\b)MLG[S]?(\\b)","\\1MG\\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\b)MICROGRAMO[S]?(\\b)","\\1MCG\\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\d+)([A-ZΜ])","\\1 \\2", od$masa_dimension)
  od$masa_dimension = gsub("(\\d*[.]?\\d+)[ ]?(MM||CM|M)?[ ]?X[ ]?(\\d*[.]?\\d+)([A-Z]+)?", "\\1 \\2 X \\3 \\4", od$masa_dimension)
  od$masa_dimension = quitar_espacios_extra(od$masa_dimension)
  
  
  od$masa_dimension2 = gsub("(\\b)MILIGRAMO[S]?(\\b)","\\1MG\\2", od$masa_dimension2)
  od$masa_dimension2 = gsub("(\\b)MILIGRAM[S]?(\\b)","\\1MG\\2", od$masa_dimension2)
  od$masa_dimension2 = gsub("(\\b)GRAMO[S]?(\\b)","\\1G\\2", od$masa_dimension2)
  od$masa_dimension2 = gsub("(\\b)GR[S]?(\\b)","\\1G\\2", od$masa_dimension2)
  od$masa_dimension2 = gsub("(\\b)MILIMETRO[S]?(\\b)","\\1MM\\2", od$masa_dimension2)
  od$masa_dimension2 = gsub("(\\b)METRO[S]?(\\b)","\\1M\\2", od$masa_dimension2)
  od$masa_dimension2 = gsub("(\\b)CENTIMETRO[S]?(\\b)","\\1CM\\2", od$masa_dimension2)
  od$masa_dimension2 = gsub("(\\b)MGR[S]?(\\b)","\\1MG\\2", od$masa_dimension2)
  od$masa_dimension2 = gsub("(\\b)MILIMETRO[S]?(\\b)","\\1MM\\2", od$masa_dimension2)
  od$masa_dimension2 = gsub("(\\b)MG[S]?(\\b)","\\1MG\\2", od$masa_dimension2)
  od$masa_dimension2 = gsub("(\\b)MLG[S]?(\\b)","\\1MG\\2", od$masa_dimension2)
  od$masa_dimension2 = gsub("(\\b)MICROGRAMO[S]?(\\b)","\\1MCG\\2", od$masa_dimension2)
  od$masa_dimension2 = gsub("(\\d+)([A-ZΜ])","\\1 \\2", od$masa_dimension2)
  od$masa_dimension2 = gsub("(\\d*[.]?\\d+)[ ]?(MM||CM|M)?[ ]?X[ ]?(\\d*[.]?\\d+)([A-Z]+)?", "\\1 \\2 X \\3 \\4", od$masa_dimension2)
  od$masa_dimension2 = quitar_espacios_extra(od$masa_dimension2)
  
  
  od$volumen = gsub("(\\b)MILILITRO[S]?(\\b)","\\1ML\\2", od$volumen)
  od$volumen = gsub("(\\b)LITRO[S]?(\\b)","\\1L\\2", od$volumen)
  od$volumen = gsub("(\\d+)([A-Z])","\\1 \\2", od$volumen)
  od$volumen = gsub("(\\d*[.]?\\d+)[ ]?(ML|L)?[ ]?X[ ]?(\\d*[.]?\\d+)([A-Z]+)?", "\\1 \\2 X \\3 \\4", od$volumen)
  od$volumen = quitar_espacios_extra(od$volumen)
  
  # PASO 29 LIMPIEZA: Eliminamos las vacías y para las prescripciones que tengan 7 o más palabras, vamos a quedarnos con las 5 primeras 
  if (length(which(od$od_limpia == "")) > 0) {
    od = od[-which(od$od_limpia == ""),]
  }
  
  contar_palabras <- function(frases) {
    palabras_por_frase <- strsplit(frases, "\\s+")
    cantidad_palabras <- sapply(palabras_por_frase, length)
    return(cantidad_palabras)
  }
  
  primeras_palabras <- function(frases, n = 5) {
    sapply(frases, function(frase) {
      palabras <- unlist(strsplit(frase, "\\s+"))     # separa las palabras por espacios
      primeras <- head(palabras, n)                   # toma las primeras n palabras
      resultado <- paste(primeras, collapse = " ")    # vuelve a unirlas en una frase
      return(resultado)
    })
  }
  
  # Para las prescripciones que tengan 7 o más palabras, vamos a quedarnos con las 5 primeras
  for (i in 1:nrow(od)) {
    num = contar_palabras(od$od_limpia[i])
    if(num > 6) {
      od$od_limpia[i] = primeras_palabras(od$od_limpia[i])
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 17 DE 46\n")
    
  }
  
  # PASO 30 LIMPIEZA: Eliminamos las palabras repetidas de las prescripciones
  for (i in 1:nrow(od)) {
    if(!is.na(od$od_limpia[i])) {
      od$od_limpia[i] = paste(unique(unlist(strsplit(od$od_limpia[i], " "))), collapse = " ")
    }
    # cat("Prescripción ",i, " de ", nrow(od), " analizada. BUCLE 18 DE 46\n")
  }
  
  # # cat("FINALIZADO EL PROCESO DE LIMPIEZA DE LAS PRESCRIPCIONES\n")
  # Sys.sleep(2)
  ################################################################################  
  ################################################################################
  # # cat("COMIENZA EL PROCESO DE CLASIFICACIÓN DE LAS PRESCRIPCIONES\n")
  # Sys.sleep(2)
  op = od
  
  # PASO 3 CLASIFICACIÓN: Con el diccionario de laboratorios creado, vamos a eliminar palabras
  # referentes a laboratorios del campo de prescripciones (excepto algunas que son genéricas y pueden corresponderse a productos)
  # Estas palabras las añadimos al campo de palabras_lab y el id del laboratorio lo añadimos en lab_id
  palabras <- unique(unlist(strsplit(diccionario_lab$name, " ")))
  palabras = setdiff(palabras,c("SALUD","AGUA","Q","BIO","NUTRICION","NATURAL","MAX","THE","REGULADORA","BEBE","MAR","CIRUGIA","NATUR","ENERGY", "HEALTH", "FLORADIX","CASEN","APOSITOS","CARE"))
  lab_no_eliminar = c("NORD","KERN","BAC","BARD","MABO",
                      "FAES"   , "LANIER"  ,"ARISTO",  "OIKO"  ,  "FARDI" ,    
                      "SEID" ,   "ISDIN",   "NARVAL",  "CORYSAN" ,"INDAS" ,  "URGO"  , 
                      "VERKOS",  "ALTER", "VICHY", "SKINCEUTICALS", "ROCHE","POSAY","CERAVE")
  
  patron = paste(palabras, collapse = "|")
  patron = paste0("(\\b(",patron,")\\b)|")
  patron = paste(patron,paste(lab_no_eliminar, collapse = "|"), sep = "")
  
  lab_no_eliminar = paste(lab_no_eliminar, collapse = "|")
  
  for (i in 1:nrow(op)) {
    if(grepl(patron,op$od_limpia[i])) {
      op$palabras_lab[i] = paste(str_extract_all(op$od_limpia[i],patron)[[1]], collapse = " ")
      if(!grepl(lab_no_eliminar,op$od_limpia[i])) {
        op$od_limpia[i] = str_remove_all(op$od_limpia[i],patron)
      }
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 19 DE 46\n")
  }
  
  
  op$od_limpia = gsub("LABORATORIO.*|EG LABO|\\bSL\\b","",op$od_limpia)
  
  # PASO 4 CLASIFICACIÓN: Si detectamos que alguna de las prescripciones contiene palabras de laboratorios
  # distintos es probable que se trate de una prescripción con más de un producto. En este caso, las vamos a eliminar
  l <- vector("list", nrow(op))
  for (i in 1:nrow(op)) {
    if(!is.na(op$palabras_lab[i])) {
      pres = unlist(strsplit(op$palabras_lab[i], " "))
      lab = strsplit(diccionario_lab$name, " ")
      for (j in 1:length(pres)) {
        for (k in 1:length(lab)) {
          if(pres[j] %in% lab[[k]]) {
            op$lab_id[i] = diccionario_lab$id[k]
            l[[i]] <- c(l[[i]],diccionario_lab$id[k])
          }
        }
      }
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 20 DE 46\n")
  }
  
  mas_un_lab = c()
  for (i in 1:nrow(op)) {
    if(length(unique(l[[i]])) > 1 && !setequal(l[[i]], c(2000279, 2000200))
       && !setequal(l[[i]], c(424, 2000032))
       && !setequal(l[[i]], c(8001, 2000059))) {
      mas_un_lab[i] = i
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 21 DE 46\n")
  }
  
  if (length(which(!is.na(mas_un_lab))) > 0) {
    op = op[-which(!is.na(mas_un_lab)),]
  }
  
  # PASO 5 CLASIFICACIÓN: Como el laboratorio L'Óreal contiene Vichy, Skinceuticals, Cerave y Roche Posay,
  # pero en la tabla laboratory_product están todos asociados a L'Óreal, recodificamos el código
  # de estos laboratorios para que tengan el de L'Óreal (2000274)
  for (i in 1:nrow(op)) {
    if(op$lab_id[i] %in% c(2000278,2000275,2000277,2000276)) {
      op$lab_id[i] = 2000274
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 22 DE 46\n")
  }
  
  op$od_limpia = quitar_espacios_extra(op$od_limpia)
  
  if (length(which(op$od_limpia == "")) > 0) {
    op = op[-which(op$od_limpia == ""),]
  }
  rownames(op) = 1:nrow(op)
  
  ##################################################################################
  #PASO 6 CLASIFICACIÓN: Vamos a calcular la distancia de Jaccard entre cada prescripción (limpia) y los medicamentos
  # Valores cercanos a 0 → Los textos son similares.
  # Valores cercanos a 1 → Los textos son muy diferentes.
  # Se calcula como dist(A,B) = 1 - (|A intersect B|/|A union B|)
  
  
  j2matriz <- stringdistmatrix(op$od_limpia, diccionario_med$med, method = "jaccard", q=2) #q = 2 para trabajar con palabras y no con caracteres
  
  # Para la matriz de jaccard con q=2
  j2minimos = apply(j2matriz, 1, min)
  j2p5 = quantile(j2minimos,0.05) # = 0
  j2p10 = quantile(j2minimos,0.1) # = 0.06666667 
  j2p15 = quantile(j2minimos,0.15) # = 0.07692308 
  j2p20 = quantile(j2minimos,0.2) # = 0.1111111 
  j2p25 = quantile(j2minimos,0.25) # = 0.125
  
  
  op$dist_j2 = rep(NA,nrow(op))
  op$j2p5 = rep(NA,nrow(op))
  op$j2p10 = rep(NA,nrow(op))
  op$j2p15 = rep(NA,nrow(op))
  op$j2p20 = rep(NA,nrow(op))
  op$j2p25 = rep(NA,nrow(op))
  
  
  for (i in 1:nrow(op)) {
    fila2 = j2matriz[i,]
    minimo2 = min(fila2)
    pos2 = which.min(fila2)
    op$dist_j2[i] = minimo2
    if (minimo2 <= j2p5) {
      op$j2p5[i] = diccionario_med$med[pos2]
    }
    if (minimo2 <= j2p10) {
      op$j2p10[i] = diccionario_med$med[pos2]
    }
    if (minimo2 <= j2p15) {
      op$j2p15[i] = diccionario_med$med[pos2]
    }
    if (minimo2 <= j2p20) {
      op$j2p20[i] = diccionario_med$med[pos2]
    }
    if (minimo2 <= j2p25) {
      op$j2p25[i] = diccionario_med$med[pos2]
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 23 DE 46\n")
  }
  
  # PASO 7 CLASIFICACIÓN: Observando las prescripciones, podemos fijar un punto de corte, tal que prescripciones
  # cuya distancia sea inferior a la fijada se clasificarán como medicamentos
  op$med_j = op$dist_j2 <= 0.425
  op$nombre_med_j = rep(NA, nrow(op))
  
  
  for (i in 1:nrow(op)) {
    if(op$med_j[i] == T) {
      fila2 = j2matriz[i,]
      pos2 = which.min(fila2)
      op$nombre_med_j[i] = diccionario_med$med[pos2]
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 24 DE 46\n")
  }
  
  op = op %>% select(-j2p5,-j2p10,-j2p15,-j2p20,-j2p25)
  
  ################################################################################
  # PASO 8 CLASIFICACIÓN: Calculamos la distancia de Levenshtein para ver qué hacemos con las que no detectamos con Jaccard
  lvmatriz <- stringdistmatrix(op$od_limpia, diccionario_med$med, method = "lv") 
  
  # Para la matriz de Levenshtein
  lvminimos = apply(lvmatriz, 1, min)
  lvp5 = quantile(lvminimos,0.05) 
  lvp10 = quantile(lvminimos,0.1) 
  lvp15 = quantile(lvminimos,0.15) 
  lvp20 = quantile(lvminimos,0.2) 
  lvp25 = quantile(lvminimos,0.25) 
  
  op$dist_lv = rep(NA,nrow(op))
  op$lvp5 = rep(NA,nrow(op))
  op$lvp10 = rep(NA,nrow(op))
  op$lvp15 = rep(NA,nrow(op))
  op$lvp20 = rep(NA,nrow(op))
  op$lvp25 = rep(NA,nrow(op))
  
  
  for (i in 1:nrow(op)) {
    fila = lvmatriz[i,]
    minimo = min(fila)
    pos = which.min(fila)
    op$dist_lv[i] = minimo
    if (minimo <= lvp5) {
      op$lvp5[i] = diccionario_med$med[pos]
    }
    if (minimo <= lvp10) {
      op$lvp10[i] = diccionario_med$med[pos]
    }
    if (minimo <= lvp15) {
      op$lvp15[i] = diccionario_med$med[pos]
    }
    if (minimo <= lvp20) {
      op$lvp20[i] = diccionario_med$med[pos]
    }
    if (minimo <= lvp25) {
      op$lvp25[i] = diccionario_med$med[pos]
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 25 DE 46\n")
  }
  
  # PASO 9 CLASIFICACIÓN: Clasificamos como medicamentos aquellas prescripciones que estén a distancia 1 o inferior
  # de un medicamento de nuestro diccionario
  op$med_lv = op$dist_j2 > 0.425 & op$dist_lv <= 1
  op$nombre_med_lv = rep(NA, nrow(op))
  
  
  for (i in 1:nrow(op)) {
    if(op$med_lv[i] == T) {
      fila = lvmatriz[i,]
      pos = which.min(fila)
      op$nombre_med_lv[i] = diccionario_med$med[pos]
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 26 DE 46\n")
  }
  
  op = op %>% select(-lvp5,-lvp10,-lvp15,-lvp20,-lvp25)
  
  ################################################################################
  # PASO 10 CLASIFICACIÓN: Buscamos entre las prescripciones palabras exactas de medicamentos (obviando las que puedan ser comunes
  palabras_med = unlist(strsplit(diccionario_med$med, " "))
  palabras_prod = unlist(strsplit(productos$presentation, " "))
  comun = intersect(palabras_med, palabras_prod)
  if(length(which(palabras_med %in% c(palabras_prod,"VIVA","DISOLV","SUPOSITORIOS","OFTALMICA",
                                      "APLICADOR","OFTALMICO","SEMANA","PRECARGADA","PASTA","SECRECION",
                                      "OCULARES","SODICO","SODIO","DISOLVENTE","BD","ANTIGRIPAL"))) > 0) {
    palabras_med = unique(palabras_med[-which(palabras_med %in% c(palabras_prod,"VIVA","DISOLV","SUPOSITORIOS","OFTALMICA",
                                                                  "APLICADOR","OFTALMICO","SEMANA","PRECARGADA","PASTA","SECRECION",
                                                                  "OCULARES","SODICO","SODIO","DISOLVENTE","BD","ANTIGRIPAL"))])
  }
  palabras_med = c("ENEMA","ENTEROSILICONA",palabras_med)
  op$med_p = rep(F,nrow(op))
  op$nombre_med_p = rep(NA, nrow(op))
  
  for (i in 1:nrow(op)) {
    if(op$med_j[i] == F & op$med_lv[i] == F) {
      palabras_pres = unlist(strsplit(op$od_limpia[i], " "))
      coincidencias = sum(palabras_pres %in% palabras_med)
      palabra = palabras_pres[which(palabras_pres %in% palabras_med)[1]]
      if(coincidencias > 0) {
        op$med_p[i] = T
        op$nombre_med_p[i] = diccionario_med[grepl(paste0("\\b",palabra,"\\b"),diccionario_med$med),1][1]
      }
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 27 DE 46\n")
  }
  
  op$med = op$med_j | op$med_lv | op$med_p
  op$nombre_med = rep(NA,nrow(op))
  
  for (i in 1:nrow(op)) {
    if(op$med_j[i]) {
      op$nombre_med[i] = op$nombre_med_j[i]
    }
    if(op$med_lv[i]) {
      op$nombre_med[i] = op$nombre_med_lv[i]
    }
    
    if(op$med_p[i]) {
      op$nombre_med[i] = op$nombre_med_p[i]
    }
    
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 28 DE 46\n")
  }
  
  op = op %>%
    left_join(diccionario_med, by=c("nombre_med" = "med"))
  
  # Buscamos ahora variedades de medicamentos
  lista <- vector("list", nrow(op))
  for (i in 1:nrow(op)) {
    lista[[i]] <- list(
      palabras = c()  
    )
  }
  
  for (i in 1:nrow(op)) {
    palabras = unlist(strsplit(op$od_limpia[i], " "))
    lista[[i]]$palabras = palabras
    
    for (j in 1:length(lista[[i]]$palabras)) {
      pj = lista[[i]]$palabras[j]
      if(grepl("Y", pj)) {
        lista[[i]]$palabras = c(lista[[i]]$palabras, gsub("Y","I",pj))
      }
    }
    for (a in 1:length(lista[[i]]$palabras)) {
      pa = lista[[i]]$palabras[a]
      if (grepl("LL", pa)) {
        lista[[i]]$palabras = c(lista[[i]]$palabras, gsub("LL","L",pa))
      }
    }
    for (b in 1:length(lista[[i]]$palabras)) {
      pb = lista[[i]]$palabras[b]
      if (grepl("TH", pb)) {
        lista[[i]]$palabras = c(lista[[i]]$palabras, gsub("TH","T",pb))
      }
    }
    for (c in 1:length(lista[[i]]$palabras)) {
      pc = lista[[i]]$palabras[c]
      if (grepl("PH", pc)) {
        lista[[i]]$palabras = c(lista[[i]]$palabras, gsub("PH","F",pc))
      }
    }
    
    for (k in 1:length(lista[[i]]$palabras)) {
      pk = lista[[i]]$palabras[k]
      if(grepl("(ENE|INE|ONE|IDE|ANE|IME)$", pk)) {
        lista[[i]]$palabras = c(lista[[i]]$palabras,str_replace(pk, ".$", "A"),str_replace(pk, ".$", "O"))
      }
      if(grepl("(ENUM)$", pk)) {
        lista[[i]]$palabras = c(lista[[i]]$palabras,str_replace(pk, "..$", "O"),str_replace(pk, "..$", "A"), str_replace(pk, "..$", "E"))
      }
      if(grepl("(IN|EN|IM|ON)$", pk)) {
        lista[[i]]$palabras = c(lista[[i]]$palabras,paste0(pk,"A"),paste0(pk,"O"),paste0(pk,"E"))
      }
      if(grepl("(AC)$", pk)) {
        lista[[i]]$palabras = c(lista[[i]]$palabras,paste0(pk,"O"))
      }
      if(grepl("OX$", pk)) {
        lista[[i]]$palabras = c(lista[[i]]$palabras,paste0(pk,"INA"),paste0(pk,"INO"),paste0(pk,"INE"))
      }
      if(grepl("(OLE|OLO)$", pk)) {
        lista[[i]]$palabras = c(lista[[i]]$palabras,str_replace(pk, ".$", ""))
      }
      if(grepl("OL$", pk)) {
        lista[[i]]$palabras = c(lista[[i]]$palabras,paste0(pk,"E"),paste0(pk,"O"))
      }
      if(grepl("IL$", pk)) {
        lista[[i]]$palabras = c(lista[[i]]$palabras,paste0(pk,"E"),paste0(pk,"O"),paste0(pk,"A"))
      }
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 29 DE 46\n")
  }
  
  palabras_med = unlist(strsplit(diccionario_med$med, " "))
  palabras_prod = unlist(strsplit(productos$presentation, " "))
  if(length(which(palabras_med %in% c(palabras_prod,"VIVA","DISOLV","SUPOSITORIOS","OFTALMICA",
                                      "APLICADOR","OFTALMICO","SEMANA","PRECARGADA","PASTA","SECRECION",
                                      "OCULARES","SODICO","SODIO","DISOLVENTE","BD","ANTIGRIPAL"))) > 0) {
    palabras_med = unique(palabras_med[-which(palabras_med %in% c(palabras_prod,"VIVA","DISOLV","SUPOSITORIOS","OFTALMICA",
                                                                  "APLICADOR","OFTALMICO","SEMANA","PRECARGADA","PASTA","SECRECION",
                                                                  "OCULARES","SODICO","SODIO","DISOLVENTE","BD","ANTIGRIPAL"))])
  }
  
  for (i in 1:nrow(op)) {
    palabras = lista[[i]]$palabras
    palabras_coin = palabras[palabras %in% c(palabras_med,"PREDNISOLONE","NITROFURANTOINA",
                                             "PREDNISOLON","BRUFEN", "HYDROCHLORIDE", "XYLOMETAZOLINE","HYDROCHLORID")]
    coincidencias = sum(palabras %in% c(palabras_med,"PREDNISOLONE","NITROFURANTOINA",
                                        "PREDNISOLON","BRUFEN", "HYDROCHLORIDE", "XYLOMETAZOLINE","HYDROCHLORID"))
    if (coincidencias > 0) {
      op$med[i] = T
      op$nombre_med[i] = diccionario_med$med[grepl(palabras_coin,diccionario_med$med)][1]
      op$estup_psicot[i] = diccionario_med$estup_psicot[grepl(palabras_coin,diccionario_med$med)][1]
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 30 DE 46\n")
  }
  
  # PASO 11 CLASIFICACIÓN: Como último paso en la detección de medicamentos, buscamos si en alguna prescripción
  # el médico añadió un CN correspondiente a un medicamento
  for (i in 1:nrow(op)) {
    if(!is.na(op$CN[i]) & op$med[i] == F) {
      CN = as.character(op$CN[i])
      CN1 = gsub("\\.\\d","",CN)
      CN2 = gsub("\\.","",CN)
      CN3 = substr(CN1, 1, nchar(CN1) - 1)
      CN4 = substr(CN1, 1, nchar(CN1) - 2)
      for (j in 1:nrow(prescripcionmed)) {
        if(CN1 == prescripcionmed$id[j]) {
          op$med[i] = T
          op$nombre_med[i] = prescripcionmed$des_nomco[j]
          op$estup_psicot[i] = nchar(prescripcionmed$lista_estupefaciente[j]) > 0 | nchar(prescripcionmed$lista_psicotropo[j]) > 0
        }
        if(CN2 == prescripcionmed$id[j]) {
          op$med[i] = T
          op$nombre_med[i] = prescripcionmed$des_nomco[j]
          op$estup_psicot[i] = nchar(prescripcionmed$lista_estupefaciente[j]) > 0 | nchar(prescripcionmed$lista_psicotropo[j]) > 0
          
        }
        if(CN3 == prescripcionmed$id[j]) {
          op$med[i] = T
          op$nombre_med[i] = prescripcionmed$des_nomco[j]
          op$estup_psicot[i] = nchar(prescripcionmed$lista_estupefaciente[j]) > 0 | nchar(prescripcionmed$lista_psicotropo[j]) > 0
          
        }
        if(CN4 == prescripcionmed$id[j]) {
          op$med[i] = T
          op$nombre_med[i] = prescripcionmed$des_nomco[j]
          op$estup_psicot[i] = nchar(prescripcionmed$lista_estupefaciente[j]) > 0 | nchar(prescripcionmed$lista_psicotropo[j]) > 0
          
        }
      }
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 31 DE 46\n")
  }
  
  ################################################################################
  # PASO 12 CLASIFICACIÓN: Vamos a tratar de clasificar aquellas prescripciones de OTROS_PRODUCTOS en las que 
  # el médico añadió algún código identificativo del producto
  op$od_limpia = quitar_espacios_extra(op$od_limpia)
  
  op$producto_id = rep(NA, nrow(op))
  op$producto_name = rep(NA, nrow(op))
  op$dist_producto = rep(NA, nrow(op))
  op$num_palabras = rep(NA, nrow(op))
  
  
  for (i in 1:nrow(op)) {
    if(op$med[i] == F & !is.na(op$CN[i])) {
      CN = as.character(op$CN[i])
      CN1 = gsub("\\.\\d","",CN)
      CN2 = gsub("\\.","",CN)
      CN3 = substr(CN1, 1, nchar(CN1) - 1)
      CN4 = substr(CN1, 1, nchar(CN1) - 2)
      for (j in 1:nrow(productos)) {
        if(CN1 == productos$nationalCode[j]) {
          op$producto_id[i] = productos$product_id[j]
          op$producto_name[i] = productos$presentation[j]
        }
        if(CN2 == productos$nationalCode[j]) {
          op$producto_id[i] = productos$product_id[j]
          op$producto_name[i] = productos$presentation[j]
        }
        if(CN3 == productos$nationalCode[j]) {
          op$producto_id[i] = productos$product_id[j]
          op$producto_name[i] = productos$presentation[j]
        }
        if(CN4 == productos$nationalCode[j]) {
          op$producto_id[i] = productos$product_id[j]
          op$producto_name[i] = productos$presentation[j]
        }
      }
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 32 DE 46\n")
  }
  
  ################################################################################
  # CLASIFICACIÓN DE AQUELLOS CON LABORATORIO
  # PASO 13 CLASIFICACIÓN: Definimos la siguiente función que nos será útil
  unir = function(palabras) {
    resultado = palabras
    if (length(palabras) == 1) {
      resultado = palabras
    }
    else {
      for (i in 1:(length(palabras) - 1)) {
        combinacion <- paste0(palabras[i], palabras[i + 1])
        resultado <- c(resultado, combinacion)
      }
    }
    return(resultado)
  }
  
  
  # PASO 14 CLASIFICACIÓN: Buscamos primero coincidencias de palabras del campo de texto libre limpio en 
  # el campo presentation de productos del laboratorio buscado
  aux_lab <- vector("list", nrow(op))
  for (i in 1:nrow(op)) {
    aux_lab[[i]] <- list(
      od_original = c(),
      producto_id = c(),
      producto_name = c(),       
      num_palabras = c()    
    )
  }
  
  for (i in 1:nrow(op)) {
    if (op$med[i] == F & is.na(op$CN[i]) & is.na(op$producto_id[i]) & !is.na(op$lab_id[i])) {
      palabras_pres <- unlist(strsplit(paste(op$od_limpia[i],sep = " "), " "))
      palabras_pres = palabras_pres[palabras_pres != ""]
      palabras_pres = palabras_pres[palabras_pres != " "]
      palabras_pres = unir(palabras_pres)
      
      lab <- op$lab_id[i]
      pal_lab = op$palabras_lab[i]
      if (lab %in% c(2000200,2000279)) {
        prod <- productos %>%
          filter(laboratory_id %in% c(2000200,2000279)) %>%
          select(product_id, presentation)
      }
      else {
        if (lab != 2000274) {
          prod <- productos %>%
            filter(laboratory_id == lab) %>%
            select(product_id, presentation)
        }
        else {
          prod <- productos %>%
            filter(laboratory_id == lab) %>%
            filter(grepl(pal_lab,presentation)) %>% 
            select(product_id, presentation)
        }
      }
      prod$presentation = gsub("[(]|[)]", "", prod$presentation)
      prod$presentation = gsub("[-]", " ", prod$presentation)
      
      for (k in 1:nrow(prod)) {
        producto <- prod$presentation[k]
        id_prod <- prod$product_id[k]
        palabras_prod <- unir(unlist(strsplit(producto, " ")))
        coincidencias <- sum(palabras_pres %in% palabras_prod)
        
        if (coincidencias > 0) {
          aux_lab[[i]]$od_original = op$od_original[i]
          aux_lab[[i]]$producto_id <- c(aux_lab[[i]]$producto_id, id_prod)
          aux_lab[[i]]$producto_name <- c(aux_lab[[i]]$producto_name, producto)
          aux_lab[[i]]$num_palabras <- c(aux_lab[[i]]$num_palabras, coincidencias)
          max = max(aux_lab[[i]]$num_palabras)
          aux_lab[[i]]$producto_id = aux_lab[[i]]$producto_id[aux_lab[[i]]$num_palabras == max(aux_lab[[i]]$num_palabras)]
          aux_lab[[i]]$producto_name = aux_lab[[i]]$producto_name[aux_lab[[i]]$num_palabras == max(aux_lab[[i]]$num_palabras)]
          aux_lab[[i]]$num_palabras = aux_lab[[i]]$num_palabras[aux_lab[[i]]$num_palabras == max(aux_lab[[i]]$num_palabras)]
        }
      }
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 33 DE 46\n")
  }
  
  
  # PASO 15 CLASIFICACIÓN: Para el caso de empates, buscamos entre las que hemos obtenido en el caso anterior,
  # las que contengan palabras relacionadas con el contenido, forma, concentración,... de cada prescripción
  # Si no encuentra ninguna coincidencia, nos quedamos con las del primer caso
  aux_lab2 =  vector("list", nrow(op))
  for (i in 1:nrow(op)) {
    aux_lab2[[i]] <- list(
      od_original = aux_lab[[i]]$od_original,
      producto_id = aux_lab[[i]]$producto_id,
      producto_name = aux_lab[[i]]$producto_name,
      num_palabras = aux_lab[[i]]$num_palabras,
      producto_id2 = c(),
      producto_name2 = c(),       
      num_palabras2 = c()    
    )
  }
  
  
  for (i in 1:nrow(op)) {
    if(length(aux_lab2[[i]]$producto_id) > 1) {
      if (op$lab_id[i] == 2000274) {
        palabras_pres <- unlist(strsplit(paste(op$contenido[i], 
                                               op$contenido2[i],
                                               op$forma[i],
                                               op$forma2[i],
                                               op$concentracion[i],
                                               op$masa_dimension[i],
                                               op$volumen[i],
                                               op$palabras_lab[i],
                                               sep = " "), " "))
      }
      else {
        palabras_pres <- unlist(strsplit(paste(op$contenido[i], 
                                               op$contenido2[i], 
                                               op$forma[i],
                                               op$forma2[i], 
                                               op$concentracion[i],
                                               op$masa_dimension[i],
                                               op$volumen[i],
                                               sep = " "), " "))
      }
      palabras_pres = palabras_pres[palabras_pres != "NA"]
      palabras_pres = palabras_pres[palabras_pres != ""]
      palabras_pres = palabras_pres[palabras_pres != " "]
      
      if (length(palabras_pres) > 0) {
        prod <- aux_lab2[[i]]$producto_name
        for (k in 1:length(prod)) {
          producto <- prod[k]
          id_prod <- aux_lab2[[i]]$producto_id[k]
          palabras_prod <- unlist(strsplit(producto, " "))
          coincidencias <- sum(palabras_pres %in% palabras_prod)
          if (coincidencias > 0) {
            aux_lab2[[i]]$producto_id2 <- c(aux_lab2[[i]]$producto_id2, id_prod)
            aux_lab2[[i]]$producto_name2 <- c(aux_lab2[[i]]$producto_name2, producto)
            aux_lab2[[i]]$num_palabras2 <- c(aux_lab2[[i]]$num_palabras2, coincidencias)
            max = max(aux_lab2[[i]]$num_palabras2)
            aux_lab2[[i]]$producto_id2 = aux_lab2[[i]]$producto_id2[aux_lab2[[i]]$num_palabras2 == max(aux_lab2[[i]]$num_palabras2)]
            aux_lab2[[i]]$producto_name2 = aux_lab2[[i]]$producto_name2[aux_lab2[[i]]$num_palabras2 == max(aux_lab2[[i]]$num_palabras2)]
            aux_lab2[[i]]$num_palabras2 = aux_lab2[[i]]$num_palabras2[aux_lab2[[i]]$num_palabras2 == max(aux_lab2[[i]]$num_palabras2)]
          }
        }
      }
      else {
        aux_lab2[[i]]$producto_id2 = aux_lab2[[i]]$producto_id 
        aux_lab2[[i]]$producto_name2 = aux_lab2[[i]]$producto_name 
      }
    }
    else {
      aux_lab2[[i]]$producto_id2 = aux_lab2[[i]]$producto_id 
      aux_lab2[[i]]$producto_name2 = aux_lab2[[i]]$producto_name 
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 34 DE 46\n")
  }
  
  # PASO 16 CLASIFICACIÓN: Por si siguen quedando empates, buscamos el producto más cercano según la distancia
  # de Jaccard
  aux_lab3 =  vector("list", nrow(op))
  for (i in 1:nrow(op)) {
    aux_lab3[[i]] <- list(
      od_original = aux_lab2[[i]]$od_original,
      producto_id = aux_lab2[[i]]$producto_id,
      producto_name = aux_lab2[[i]]$producto_name,
      num_palabras = aux_lab2[[i]]$num_palabras,
      producto_id2 =aux_lab2[[i]]$producto_id2,
      producto_name2 = aux_lab2[[i]]$producto_name2,       
      num_palabras2 = aux_lab2[[i]]$num_palabras2,
      producto_id3 = c(),
      producto_name3 = c(),
      l = c(),
      producto_dist3 = c()
    )
  }
  
  for (i in 1:nrow(op)) {
    if(length(aux_lab3[[i]]$producto_id2) > 1) {
      if (op$lab_id[i] == 2000274) {
        palabras_pres <- paste(op$od_limpia[i],
                               op$palabras_lab[i],
                               sep = " ")
      }
      else {
        palabras_pres <- op$od_limpia[i]
      }
      palabras_pres = gsub("\\bNA\\b","",palabras_pres)
      palabras_pres = quitar_espacios_extra(palabras_pres)
      
      ids_prod = aux_lab3[[i]]$producto_id2
      dist = c()
      for (k in 1:length(ids_prod)){
        prod <- productos[productos$product_id == aux_lab3[[i]]$producto_id2[k],"presentation2"]
        distancia = stringdistmatrix(palabras_pres, prod, method = "jaccard", q=2) 
        dist[k] = distancia
      }
      minimo = min(dist)
      pminimo = which.min(dist)
      
      aux_lab3[[i]]$producto_id3 = aux_lab3[[i]]$producto_id2[pminimo] 
      aux_lab3[[i]]$producto_name3 = aux_lab3[[i]]$producto_name2[pminimo]
      aux_lab3[[i]]$producto_dist3 = minimo 
    }
    else {
      aux_lab3[[i]]$producto_id3 = aux_lab3[[i]]$producto_id2 
      aux_lab3[[i]]$producto_name3 = aux_lab3[[i]]$producto_name2 
      if(length(aux_lab3[[i]]$producto_id2) == 1) {
        if (op$lab_id[i] == 2000274) {
          palabras_pres <- paste(op$od_limpia[i],
                                 op$palabras_lab[i],
                                 sep = " ")
        }
        else {
          palabras_pres <- op$od_limpia[i]
        }
        
        palabras_pres = gsub("\\bNA\\b","",palabras_pres)
        palabras_pres = quitar_espacios_extra(palabras_pres)
        aux_lab3[[i]]$producto_dist3 = stringdistmatrix(palabras_pres, aux_lab3[[i]]$producto_name2, method = "jaccard", q=2) 
        aux_lab3[[i]]$num_palabras3 = sum(unlist(strsplit(palabras_pres, " ")) %in% unlist(strsplit(aux_lab3[[i]]$producto_name2, " ")))
        
      }
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 35 DE 46\n")
  }
  
  # PASO 17 CLASIFICACIÓN: Asignamos cada prescripción de la que tengamos un producto asociado a ese producto
  for (i in 1:nrow(op)) {
    if(length(aux_lab3[[i]]$producto_id3) == 1) {
      op$producto_id[i] = aux_lab3[[i]]$producto_id3
      op$producto_name[i] = aux_lab3[[i]]$producto_name3
      op$dist_producto[i] = aux_lab3[[i]]$producto_dist3
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 36 DE 46\n")
  }
  
  ################################################################################
  # CLASIFICACIÓN DEL RESTO DE PRODUCTOS
  # PASO 18 CLASIFICACIÓN: Definimos la siguiente función de distancia entre palabras
  distancia = function(a,b) {
    pa = unlist(strsplit(a, " "))
    pb = unlist(strsplit(b, " "))
    union = union(pa,pb)
    interseccion = intersect(pa,pb)
    dist = 1 - (length(interseccion)/length(union))
    return(dist)
  }
  
  a = nrow(op)
  
  quitar = c("HIDRATANTE","FACIAL","PLUS","FORTE","VAGINAL","VAGINALES","FACIAL","MANOS",
             "CLORHEXIDINA","PROBIOTICO","PROBIOTICOS","INTIMA","LIMPIADOR","ACEITE","COMPLEX",
             "PRO","FUERTE","TOTAL","SOL","FLEXIBLE","LOCION","GELATINA","BLANDA","ESTERIL",
             "SONDA","ALGODON","GRANDE","LARGA","GASA","BOLSA","TALLA","EXTRA","PEQUEÑA","COMPRESAS",
             "MUJER","LECHE","POLVO","MIEL","LIGHT","BARRITAS","BATIDO","PAPILLA","NOCHE","SUEÑO","NEUTRO",
             "TRANSPARENTE","RETORNO","PROBIOTIC","STICKS","VEGETAL","VEGETALES","CHUPAR","LAGRIMAS","ARTIFICIALES",
             "ENZIMAS","DIGESTIVO","DIGESTIVAS","DIGESTIVOS","DIGESTIVA","ALOE","VERA","AMPOLLAS","BEBIBLES","ACTIVE",
             "RETINOL","ACIDO","HIALURONICO","LAVANDA","SERUM","MEDIA","COMPRESION","RODILLAS","B","C","D","OVULOS")
  
  # PASO 19 CLASIFICACIÓN: Buscamos primero coincidencias de palabras del campo de texto libre limpio en 
  # el campo presentation de productos 
  aux <- vector("list", a)
  for (i in 1:a) {
    aux[[i]] <- list(
      od_original = c(),
      producto_id = c(),
      producto_name = c(),       
      num_palabras = c()    
    )
  }
  
  for (i in 1:a) {
    if (!op$med[i] & is.na(op$CN[i]) & is.na(op$lab_id[i]) & is.na(op$producto_id[i])) {
      palabras_pres1 <- unlist(strsplit(paste(op$od_limpia[i],sep = " "), " "))
      palabras_pres1 = palabras_pres1[palabras_pres1 != ""]
      palabras_pres1 = palabras_pres1[palabras_pres1 != " "]
      palabras_pres = palabras_pres1[!nchar(palabras_pres1) == 1]
      palabras_pres = setdiff(palabras_pres,quitar)
      palabras_pres = unir(palabras_pres)
      
      for (k in 1:nrow(productos)) {
        producto <- productos$presentation[k]
        id_prod <- productos$product_id[k]
        palabras_prod <- unlist(strsplit(producto, " "))
        palabras_prod = setdiff(palabras_prod,quitar) 
        palabras_prod = unir(palabras_prod) 
        
        coincidencias <- sum(palabras_pres %in% palabras_prod)
        if (coincidencias > 0) {
          aux[[i]]$od_original = op$od_original[i]
          aux[[i]]$producto_id <- c(aux[[i]]$producto_id, id_prod)
          aux[[i]]$producto_name <- c(aux[[i]]$producto_name, producto)
          aux[[i]]$num_palabras <- c(aux[[i]]$num_palabras, coincidencias)
          max = max(aux[[i]]$num_palabras)
          aux[[i]]$producto_id = aux[[i]]$producto_id[aux[[i]]$num_palabras == max(aux[[i]]$num_palabras)]
          aux[[i]]$producto_name = aux[[i]]$producto_name[aux[[i]]$num_palabras == max(aux[[i]]$num_palabras)]
          aux[[i]]$num_palabras = aux[[i]]$num_palabras[aux[[i]]$num_palabras == max(aux[[i]]$num_palabras)]
        }
      }
    }
    # cat("Prescripción ",i, " de ", a, " analizada. BUCLE 37 DE 46\n")
  }
  
  
  
  # PASO 20 CLASIFICACIÓN: Para el caso de empates, buscamos entre las que hemos obtenido en el caso anterior,
  # las que contengan palabras relacionadas con el contenido, forma, concentración,... de cada prescripción
  # Si el médico no escribió nada de esos campos, nos quedamos con las del primer caso
  aux2 =  vector("list", a)
  for (i in 1:a) {
    aux2[[i]] <- list(
      od_original = aux[[i]]$od_original,
      producto_id = aux[[i]]$producto_id,
      producto_name = aux[[i]]$producto_name,
      num_palabras = aux[[i]]$num_palabras,
      producto_id2 = c(),
      producto_name2 = c(),       
      num_palabras2 = c()    
    )
  }
  
  
  for (i in 1:a) {
    if(length(aux2[[i]]$producto_id) > 1) {
      palabras_pres <- unlist(strsplit(paste(op$contenido[i], 
                                             op$contenido2[i], 
                                             op$forma[i],
                                             op$forma2[i], 
                                             op$concentracion[i],
                                             op$masa_dimension[i],
                                             op$volumen[i],
                                             sep = " "), " "))
      palabras_pres = palabras_pres[palabras_pres != "NA"]
      palabras_pres = palabras_pres[palabras_pres != ""]
      palabras_pres = palabras_pres[palabras_pres != " "]
      
      if (length(palabras_pres) > 0) {
        prod <- aux2[[i]]$producto_name
        for (k in 1:length(prod)) {
          producto <- prod[k]
          id_prod <- aux2[[i]]$producto_id[k]
          palabras_prod <- unlist(strsplit(producto, " "))
          
          
          if(any((palabras_prod) %in% c("COMPRIMIDOS","CAPSULAS"))){
            palabras_prod = unique(c(palabras_prod,"COMPRIMIDOS","CAPSULAS"))
          }
          
          coincidencias <- sum(palabras_pres %in% palabras_prod)
          if (coincidencias > 0) {
            aux2[[i]]$producto_id2 <- c(aux2[[i]]$producto_id2, id_prod)
            aux2[[i]]$producto_name2 <- c(aux2[[i]]$producto_name2, producto)
            aux2[[i]]$num_palabras2 <- c(aux2[[i]]$num_palabras2, coincidencias)
            max = max(aux2[[i]]$num_palabras2)
            aux2[[i]]$producto_id2 = aux2[[i]]$producto_id2[aux2[[i]]$num_palabras2 == max(aux2[[i]]$num_palabras2)]
            aux2[[i]]$producto_name2 = aux2[[i]]$producto_name2[aux2[[i]]$num_palabras2 == max(aux2[[i]]$num_palabras2)]
            aux2[[i]]$num_palabras2 = aux2[[i]]$num_palabras2[aux2[[i]]$num_palabras2 == max(aux2[[i]]$num_palabras2)]
          }
        }
      }
      else {
        aux2[[i]]$producto_id2 = aux2[[i]]$producto_id 
        aux2[[i]]$producto_name2 = aux2[[i]]$producto_name 
      }
    }
    else {
      aux2[[i]]$producto_id2 = aux2[[i]]$producto_id 
      aux2[[i]]$producto_name2 = aux2[[i]]$producto_name 
    }
    # cat("Prescripción ",i, " de ", a, " analizada. BUCLE 38 DE 46\n")
  }
  
  # PASO 21 CLASIFICACIÓN: Por si siguen quedando empates, buscamos el producto más cercano según la distancia
  # de Jaccard
  aux3 =  vector("list", a)
  for (i in 1:a) {
    aux3[[i]] <- list(
      od_original = aux2[[i]]$od_original,
      producto_id = aux2[[i]]$producto_id,
      producto_name = aux2[[i]]$producto_name,
      num_palabras = aux2[[i]]$num_palabras,
      producto_id2 =aux2[[i]]$producto_id2,
      producto_name2 = aux2[[i]]$producto_name2,       
      num_palabras2 = aux2[[i]]$num_palabras2,
      producto_id3 = c(),
      producto_name3 = c(),
      producto_dist3 = c(),
      producto_distancia = c()
    )
  }
  
  for (i in 1:a) {
    palabras_pres <- op$od_limpia[i]
    palabras_pres = quitar_espacios_extra(palabras_pres)
    
    if(length(aux3[[i]]$producto_id2) > 1) {
      ids_prod = aux3[[i]]$producto_id2
      distj = c()
      for (k in 1:length(ids_prod)){
        prod <- productos[productos$product_id == aux3[[i]]$producto_id2[k],"presentation3"]
        distanciaj = stringdistmatrix(palabras_pres, prod, method = "jaccard", q=2) 
        distj[k] = distanciaj
      }
      
      minimo = min(distj)
      pminimo = which.min(distj)
      
      aux3[[i]]$producto_id3 = aux3[[i]]$producto_id2[pminimo] 
      aux3[[i]]$producto_name3 = aux3[[i]]$producto_name2[pminimo]
      aux3[[i]]$producto_dist3 = minimo
    }
    else {
      aux3[[i]]$producto_id3 = aux3[[i]]$producto_id2 
      aux3[[i]]$producto_name3 = aux3[[i]]$producto_name2
      
      if(length(aux3[[i]]$producto_id2) == 1) {
        prod = productos[productos$product_id == aux3[[i]]$producto_id2,"presentation3"]
        aux3[[i]]$producto_dist3 = stringdistmatrix(palabras_pres, prod, method = "jaccard", q=2) 
      }
    }
    # cat("Prescripción ",i, " de ", a, " analizada. BUCLE 39 DE 46\n")
  }
  
  op$dist_producto2 = rep(NA, nrow(op))
  
  # PASO 22 CLASIFICACIÓN: Asignamos cada prescripción de la que tengamos un producto asociado a ese producto
  for (i in 1:a) {
    if(length(aux3[[i]]$producto_id3) == 1) {
      op$producto_id[i] = aux3[[i]]$producto_id3
      op$producto_name[i] = aux3[[i]]$producto_name3
      op$dist_producto[i] = aux3[[i]]$producto_dist3
      op$dist_producto2[i] = distancia(op$od_limpia[i], productos[productos$product_id == op$producto_id[i],"presentation3"])
      
    }
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 40 DE 46\n")
  }
  
  # PASO 23 CLASIFICACIÓN: Vamos a fijar los siguientes puntos de corte para la clasificación de OTROS_PRODUCTOS
  for (i in 1:nrow(op)) {
    if(!is.na(op$dist_producto[i]) &&
       op$dist_producto[i] > 0.524 &&  
       !is.na(op$dist_producto2[i]) &&
       op$dist_producto2[i] > 0.5) {
      op$producto_id[i] = NA
      op$producto_name[i] = NA
    }
    
    # cat("Prescripción ",i, " de ", nrow(op), " analizada. BUCLE 41 DE 46\n")
    
  }
  
  # PASO 24 CLASIFICACIÓN: Nos quedamos con las siguientes variables y creamos presentation_type para 
  # que el join se realice de forma correcta con la tabla original
  op = op %>% select(od_original, otro_idioma,vacuna_fmagistral, mult_productos, CN, palabras_lab, lab_id, med, nombre_med, estup_psicot, producto_id, producto_name)
  op$presentation_type = rep(NA, nrow(op))
  
  datos_clasificados = op %>% 
    right_join(datos_op, by = c("od_original" = "others_description", "presentation_type" = "presentation_type"))
  
  # write.csv(datos_clasificados, "aux1.csv", row.names = FALSE)
  # datos_clasificados <- read.csv("aux1.csv", header = TRUE)
  
  # PASO 25 CLASIFICACIÓN: Escribimos el nationalCode de los productos que hemos conseguido clasificar
  variables = names(datos_clasificados)
  if ("medicine_id" %in% variables) {
    for(i in 1:nrow(datos_clasificados)) {
      if(!is.na(datos_clasificados$producto_id[i])){
        nc = products[products$id == datos_clasificados$producto_id[i],"nationalCode"]
        if(length(nc) > 0){
          datos_clasificados$medicine_id[i] = nc
        }
      }
      # cat("Prescripción ",i, " de ", nrow(datos_clasificados), " analizada. BUCLE 42 DE 46\n")
    }
  }
  
  # PASO 26 CLASIFICACIÓN: Escribimos el id y nombres de los laboratorios de los productos que hemos conseguido clasificar
  if ("laboratory_code" %in% variables & "laboratory_name" %in% variables) {
    for (i in 1:nrow(datos_clasificados)) {
      if(!is.na(datos_clasificados$producto_id[i])){
        datos_clasificados$laboratory_code[i] = laboratory_product[laboratory_product$product_id == datos_clasificados$producto_id[i],"laboratory_id"][1]
        datos_clasificados$laboratory_name[i] = laboratories[laboratories$code == datos_clasificados$laboratory_code[i],"name"]
      }
      if(!is.na(datos_clasificados$producto_id[i])){
        datos_clasificados$producto_name[i] = products[products$id == datos_clasificados$producto_id[i],"presentation"]
      }
      # cat("Prescripción ",i, " de ", nrow(datos_clasificados), " analizada. BUCLE 43 DE 46\n")
    }
  }
  
  # PASO 27 CLASIFICACIÓN: Obtenemos los datos finales
  variables = names(datos_clasificados)
  datos_clasificados = datos_clasificados %>% 
    select(id, "product_id" = producto_id, "product_presentation" = producto_name, med, nombre_med, estup_psicot, otro_idioma, vacuna_fmagistral, mult_productos,
           "others_description" = od_original,variables[-which(variables %in% c("id","producto_id", "producto_name", "med", "nombre_med", "estup_psicot", "otro_idioma", "vacuna_fmagistral", "mult_productos", "od_original"))])
  
  # PASO EXTRA CLASIFICACIÓN
  datos_clasificados$no_clasificable = rep(F, nrow(datos_clasificados))
  for (i in 1:nrow(datos_clasificados)) {
    if(is.na(datos_clasificados$presentation_type[i]) & !is.na(datos_clasificados$med[i]) & !is.na(datos_clasificados$otro_idioma[i]) & !is.na(datos_clasificados$vacuna_fmagistral[i])){
      if(!datos_clasificados$med[i] & is.na(datos_clasificados$product_id[i]) & 
         (datos_clasificados$otro_idioma[i] | datos_clasificados$mult_productos[i])) {
        datos_clasificados$no_clasificable[i] = T
      }
    }
    if(is.na(datos_clasificados$presentation_type[i]) & !is.na(datos_clasificados$vacuna_fmagistral[i])){
      if(datos_clasificados$vacuna_fmagistral[i]){
        datos_clasificados$med[i] = F
        datos_clasificados$nombre_med[i] = NA
        datos_clasificados$estup_psicot[i] = NA
        datos_clasificados$product_id[i] = NA
        datos_clasificados$product_presentation[i] = NA
      }
      
    }
    # cat("Prescripción ",i, " de ", nrow(datos_clasificados), " analizada. BUCLE 44 DE 46\n")
  }
  
  variables = names(datos_clasificados)
  datos_clasificados = datos_clasificados %>% 
    select(id, product_id, product_presentation, med, nombre_med, estup_psicot, otro_idioma, vacuna_fmagistral, mult_productos, no_clasificable,
           others_description,variables[-which(variables %in% c("id","product_id", "producto_presentation", "med", "nombre_med", "estup_psicot", "od_original"))])
  
  # # cat("FINALIZADO EL PROCESO DE CLASIFICACIÓN DE LAS PRESCRIPCIONES\n")
  # Sys.sleep(2)
  
  ################################################################################
  # # cat("PREPARANDO INFORME Y ARCHIVOS DE RESULTADOS\n")
  
  # PASO 8: Obtenemos la información de OTROS_PRODUCTOS que contenían National Code
  # en la prescripción
  # arc_nationalCode = datos_clasificados %>% filter(!is.na(CN)) %>% filter(!med) %>% filter(!vacuna_fmagistral) %>% select(id,product_id, product_presentation,CN,others_description)
  # nom_arc_nationalCode  = paste0("NATIONALCODE_",salida,".xlsx")
  # write_xlsx(arc_nationalCode, paste0(salida,"/",nom_arc_nationalCode))
  # num_nationalCode = nrow(arc_nationalCode)
  # num_nationalCode_clasificados = nrow(arc_nationalCode %>% filter(!is.na(product_id)))
  # num_nationalCode_noclasificados = nrow(arc_nationalCode %>% filter(is.na(product_id)))
  
  # PASO 9: Obtenemos la información de OTROS_PRODUCTOS que información del laboratorio
  # en la prescripción (y no contenían National) 
  # arc_lab = datos_clasificados %>% filter(is.na(CN)) %>% filter(!med) %>% filter(!vacuna_fmagistral)  %>% filter(!is.na(lab_id)) %>% select(id,product_id, product_presentation,lab_id,others_description)
  # nom_arc_lab  = paste0("LAB_",salida,".xlsx")
  # write_xlsx(arc_lab, paste0(salida,"/",nom_arc_lab))
  # num_lab = nrow(arc_lab)
  # num_lab_clasificados = nrow(arc_lab %>% filter(!is.na(product_id)))
  # num_lab_noclasificados = nrow(arc_lab %>% filter(is.na(product_id)))
  
  # PASO 10: Obtenemos la información de las prescripciones no clasificables
  # arc_noClasificables = datos_clasificados %>% filter(no_clasificable)  %>% select(id, others_description)
  # nom_arc_noClasificables  = paste0("NO_CLASIFICABLES_",salida,".xlsx")
  # write_xlsx(arc_noClasificables, paste0(salida,"/",nom_arc_noClasificables))
  # num_noClasificables = nrow(arc_noClasificables)
  
  
  # PASO 11: Obtenemos la información de las prescripciones detectadas como medicamentos
  # num_med = datos_clasificados %>% filter(med) %>% count() %>% as.integer()
  # num_nomed = datos_clasificados %>% filter(!med) %>% filter(!vacuna_fmagistral) %>% filter(!no_clasificable) %>% count() %>% as.integer()
  # num_estpsi = datos_clasificados %>% filter(estup_psicot) %>% count() %>% as.integer()
  # arc_med = datos_clasificados %>% filter(med) %>% select(id, nombre_med, estup_psicot, others_description, medical_license_number, created_date)
  # nom_arc_med = paste0(salida,"/MEDICAMENTOS_",salida,".xlsx")
  # write_xlsx(arc_med, nom_arc_med)
  
  # PASO 12: Obtenemos la información de vacunas y fórmulas magistrales
  # num_vac_fm = datos_clasificados %>% filter(vacuna_fmagistral) %>% count() %>% as.integer()
  # num_no_vac_fm = datos_clasificados %>% filter(!vacuna_fmagistral) %>% count() %>% as.integer()
  # arc_vac_fm = datos_clasificados %>% filter(vacuna_fmagistral) %>% select(id, others_description, medical_license_number, created_date)
  # nom_arc_vac_fm = paste0(salida,"/VACUNAS_FMAGISTRALES_",salida,".xlsx")
  # write_xlsx(arc_vac_fm, nom_arc_vac_fm)
  
  # PASO 13: Obtenemos la clasificación de OTROS_PRODUCTOS
  # num_clasificados = datos_clasificados %>% filter(is.na(presentation_type)) %>% filter(!med) %>% filter(!vacuna_fmagistral) %>% filter(!no_clasificable) %>% filter(!is.na(product_id)) %>% count() %>% as.integer()
  # num_noclasificados = datos_clasificados %>% filter(is.na(presentation_type)) %>% filter(!med) %>% filter(!vacuna_fmagistral) %>% filter(!no_clasificable) %>% filter(is.na(product_id)) %>% count() %>% as.integer()
  # nom_arc = paste0("OTROS_PRODUCTOS_",salida,".xlsx")
  # write_xlsx(datos_clasificados, paste0(salida,"/",nom_arc))
  
  # PASO 14: Calculamos el tiempo de finalización y la duración de ejecución
  # fin = Sys.time()
  # duracion = difftime(fin, inicio, units = "mins")
  
  # PASO 15: Obtenemos la frecuencia de los no clasificados
  # arc_frec = datos_clasificados %>% filter(is.na(presentation_type)) %>% filter(!med) %>% filter(!vacuna_fmagistral) %>% filter(!no_clasificable) %>% filter(is.na(product_id)) %>% select(others_description) %>% count(others_description, name = "frecuencia", sort = T)
  # nom_frec = paste0("FRECUENCIA_NO_CLASIFICADOS_",salida,".xlsx")
  # write_xlsx(arc_frec, paste0(salida,"/",nom_frec))
  
  # PASO 16: Obtenemos los datos originales con toda la información generada
  # Se recomienda comentar estas 5 líneas si existe un gran número de prescripciones
  # En ese caso, quitar también la línea del siguiente cat donde se menciona "nom"
  prescription_completo = datos_clasificados %>% right_join(prescription, by="id")
  prescription_completo = prescription_completo %>% select(id, product_id, product_presentation,
                                                           med, nombre_med, estup_psicot,
                                                           otro_idioma, vacuna_fmagistral,
                                                           mult_productos, CN, palabras_lab,lab_id,
                                                           no_clasificable, ends_with(".y"))
  
  prescription_completo[] <- lapply(prescription_completo, function(x) {
    if (is.character(x) | is.factor(x)) {
      x[x == ""] <- NA
    }
    return(x)
  })
  names(prescription_completo) = gsub("[.]y","",names(prescription_completo))
  
  variables = names(prescription_completo)
  if ("medicine_id" %in% variables) {
    for(i in 1:nrow(prescription_completo)) {
      if(!is.na(prescription_completo$product_id[i])){
        nc = products[products$id == prescription_completo$product_id[i],"nationalCode"]
        if(length(nc) > 0){
          prescription_completo$medicine_id[i] = nc
        }
      }
      # cat("Prescripción ",i, " de ", nrow(prescription_completo), " analizada. BUCLE 45 DE 46\n")
    }
  }
  
  # write.csv(prescription_completo, "aux2.csv", row.names = FALSE)
  # prescription_completo <- read.csv("aux2.csv", header = TRUE)
  
  if ("laboratory_code" %in% variables & "laboratory_name" %in% variables) {
    for (i in 1:nrow(prescription_completo)) {
      if(!is.na(prescription_completo$product_id[i])){
        prescription_completo$laboratory_code[i] = laboratory_product[laboratory_product$product_id == prescription_completo$product_id[i],"laboratory_id"][1]
        prescription_completo$laboratory_name[i] = laboratories[laboratories$code == prescription_completo$laboratory_code[i],"name"]
      }
      if(!is.na(prescription_completo$product_id[i])){
        prescription_completo$product_presentation[i] = products[products$id == prescription_completo$product_id[i],"presentation"]
      }
      # cat("Prescripción ",i, " de ", nrow(prescription_completo), " analizada. BUCLE 46 DE 46\n")
    }
  }
  
  prescription_completo = prescription_completo %>% 
    arrange(desc(created_date))
    
  write.csv(prescription_completo, "resultado_clasificacion.csv", row.names = FALSE)
  
  # nom = paste0("CLASIFICACION_",salida,".xlsx")
  # write_xlsx(prescription_completo, paste0(salida,"/",nom))
  
  # PASO 17: Generamos el archivo de metadatos
  #   # cat(paste0(
  #   "METADATOS DE LOS ARCHIVOS GENERADOS POR LA FUNCIÓN clasifica()\n\n",
  #   "- INFORME_prescription_", salida, ".txt: contiene información detallada del proceso de clasificación y el tiempo de ejecución de la función. Se recomienda su lectura para conocer todos los detalles y el porcentaje de prescripciones que eran de OTROS_PRODUCTOS, aquellas que han sido por texto libre, las clasificadas como medicamentos, aquellas que se han clasificado en productos del catálogo, etc.\n\n",
  #   "- MEDICAMENTOS_prescription_", salida, ".xlsx: información de las prescripciones de OTROS_PRODUCTOS por texto libre que han sido detectadas como medicamentos. Incluye las siguientes variables:\n",
  #   "  + id: id de la prescripción.\n",
  #   "  + nombre_med: nombre del medicamento perteneciente a nuestro diccionario de medicamentos en el cual se ha clasificado la prescripción.\n",
  #   "  + estup_psicot: variable lógica que indica si el medicamento detectado es un estupefaciente o psicótropo.\n",
  #   "  + others_description: prescripción original introducida por el médico en el campo de texto libre.\n",
  #   "  + medical_license_number: número de colegiado del prescriptor.\n",
  #   "  + created_date: fecha de creación de la prescripción.\n\n",
  #   
  #   "- NO_CLASIFICABLES_prescription_", salida, ".xlsx: información de aquellas prescripciones que no ha sido posible clasificarlas debido a que se ha detectado que, o bien eran pertenecientes a otro idioma, o bien contenían múltiples productos. Sus variables:\n",
  #   "  + id: id de la prescripción.\n",
  #   "  + others_description: prescripción original introducida por el médico en el campo de texto libre.\n\n",
  #   
  #   "- VACUNAS_FMAGISTRALES_prescription_", salida, ".xlsx: información de aquellas prescripciones detectadas como vacunas o fórmulas magistrales del campo de texto libre. Contiene los campos:\n",
  #   "  + id: id de la prescripción.\n",
  #   "  + others_description: prescripción original introducida por el médico en el campo de texto libre.\n",
  #   "  + medical_license_number: número de colegiado del prescriptor.\n",
  #   "  + created_date: fecha de creación de la prescripción.\n\n",
  #   
  #   "- OTROS_PRODUCTOS_prescription_", salida, ".xlsx: información de todas las prescripciones de OTROS_PRODUCTOS, tanto las que se han conseguido clasificar, como las que no, como las detectadas como medicamentos, etc. Sus variables son:\n",
  #   "  + id: id de la prescripción.\n",
  #   "  + product_id: id de los productos para las prescripciones por texto libre que se han clasificado en alguno del catálogo.\n",
  #   "  + product_presentation: presentación de los productos para las prescripciones por texto libre que se han clasificado en alguno del catálogo.\n",
  #   "  + med: campo lógico que indica si la prescripción por texto libre se ha detectado como medicamento.\n",
  #   "  + nombre_med: nombre del medicamento perteneciente a nuestro diccionario de medicamentos en el cual se ha clasificado la prescripción.\n",
  #   "  + estup_psicot: variable lógica que indica si el medicamento detectado es un estupefaciente o psicótropo.\n",
  #   "  + otro_idioma: variable lógica que indica si la prescripción está en otro idioma.\n",
  #   "  + vacuna_fmagistral: variable lógica que indica si la prescripción ha sido detectada como vacuna o fórmula magistral.\n",
  #   "  + mult_productos: variable lógica que indica si la prescripción contiene múltiples productos.\n",
  #   "  + no_clasificable: variable lógica que indica si la prescripción es no clasificable.\n",
  #   "  + others_description: prescripción original introducida por el médico en el campo de texto libre.\n",
  #   "  + CN: código nacional identificado en las prescripciones por texto libre.\n",
  #   "  + palabras_lab: palabras relacionadas con laboratorio que se han detectado en las prescripciones por texto libre.\n",
  #   "  + lab_id: id de los laboratorios que se han detectado en las prescripciones por texto libre.\n",
  #   "  + VARIABLES QUE CONTIENEN LOS DATOS ORIGINALES\n\n",
  #   
  #   "- NATIONALCODE_prescription_", salida, ".xlsx: información de las prescripciones por texto libre en las que se ha detectado algún código identificativo del producto. Sus campos:\n",
  #   "  + id: id de la prescripción.\n",
  #   "  + product_id: id de los productos para las prescripciones por texto libre que se han clasificado en alguno del catálogo.\n",
  #   "  + product_presentation: presentación de los productos para las prescripciones por texto libre que se han clasificado en alguno del catálogo.\n",
  #   "  + CN: código nacional identificado en las prescripciones por texto libre.\n",
  #   "  + others_description: prescripción original introducida por el médico en el campo de texto libre.\n\n",
  #   
  #   "- LAB_prescription_", salida, ".xlsx: información de las prescripciones en las que se ha detectado palabras relacionadas con el laboratorio al que pertenece el producto. Sus variables:\n",
  #   "  + id: id de la prescripción.\n",
  #   "  + product_id: id de los productos para las prescripciones por texto libre que se han clasificado en alguno del catálogo.\n",
  #   "  + product_presentation: presentación de los productos para las prescripciones por texto libre que se han clasificado en alguno del catálogo.\n",
  #   "  + lab_id: id de los laboratorios que se han detectado en las prescripciones por texto libre.\n",
  #   "  + others_description: prescripción original introducida por el médico en el campo de texto libre.\n\n",
  #   
  #   "- CLASIFICACION_prescription_", salida, ".xlsx: información de la clasificación de todas las prescripciones introducidas en la función. En el código se especifican las líneas que se pueden comentar para que este archivo no se genere (puede tardar mucho debido al gran número de prescripciones que puede contener). Sus variables son:\n",
  #   "  + id: id de la prescripción.\n",
  #   "  + product_id: id de los productos para las prescripciones por texto libre que se han clasificado en alguno del catálogo.\n",
  #   "  + product_presentation: presentación de los productos para las prescripciones por texto libre que se han clasificado en alguno del catálogo.\n",
  #   "  + med: campo lógico que indica si la prescripción por texto libre se ha detectado como medicamento.\n",
  #   "  + nombre_med: nombre del medicamento perteneciente a nuestro diccionario de medicamentos en el cual se ha clasificado la prescripción.\n",
  #   "  + estup_psicot: variable lógica que indica si el medicamento detectado es un estupefaciente o psicótropo.\n",
  #   "  + otro_idioma: variable lógica que indica si la prescripción está en otro idioma.\n",
  #   "  + vacuna_fmagistral: variable lógica que indica si la prescripción ha sido detectada como vacuna o fórmula magistral.\n",
  #   "  + mult_productos: variable lógica que indica si la prescripción contiene múltiples productos.\n",
  #   "  + no_clasificable: variable lógica que indica si la prescripción es no clasificable.\n",
  #   "  + others_description: prescripción original introducida por el médico en el campo de texto libre.\n",
  #   "  + CN: código nacional identificado en las prescripciones por texto libre.\n",
  #   "  + palabras_lab: palabras relacionadas con laboratorio que se han detectado en las prescripciones por texto libre.\n",
  #   "  + lab_id: id de los laboratorios que se han detectado en las prescripciones por texto libre.\n",
  #   "  + VARIABLES QUE CONTIENEN LOS DATOS ORIGINALES\n\n",
  #   
  #   "- FRECUENCIA_NO_CLASIFICADOS_prescription_", salida, ".xlsx: para las prescripciones de OTROS_PRODUCTOS que no ha sido posible clasificarlas, se saca un archivo donde se detalla el número de repeticiones de estas prescripciones (para ver cuáles son las más repetidas). Sus campos son:\n",
  #   "  + others_description: prescripción original introducida por el médico en el campo de texto libre.\n",
  #   "  + frecuencia: número de prescripciones en el periodo analizado.\n"),
  #   file = paste0(salida,"/METADATOS.txt"))
  # 
  # 
  # # cat("INFORME ",toupper(salida),":\n",
  #     "TIEMPO DE EJECUCIÓN: ",round(duracion,2), " mins \n\n",
  #     "Los datos proporcionados constan de ", num_pres, " prescripciones, de las cuales ", num_op," (",round(100*num_op/num_pres,2),"%) ", "son de OTROS_PRODUCTOS cumpliendo los filtros necesarios.\n",
  #     "De entre las ", num_op, " prescripciones de OTROS_PRODUCTOS, ",num_cat," (",round(100*num_cat/num_op,2),"%) ", "han sido por catálogo, mientras que ",num_nocat," (",round(100*num_nocat/num_op,2),"%) ", "han sido por texto libre.\n\n",
  #     "Detallamos a continuación información acerca de estas ",num_nocat, " prescripciones por texto libre:\n",
  #     " * Se han detectado un total de ",num_med, " prescripciones correspondientes a medicamentos, esto constituye un ",round(100*num_med/num_nocat,2),"% de las prescripciones por texto libre.\n   Además, de entre estas ", num_med," prescripciones de medicamentos, ", num_estpsi, " (",round(100*num_estpsi/num_med,2),"%)", " son de estupefacientes o psicótropos.\n   SE PUEDEN CONSULTAR LAS PRESCRIPCIONES DE MEDICAMENTOS DETECTADAS EN EL ARCHIVO ",nom_arc_med,"\n\n",
  #     " * Un total de ", num_noClasificables, " se han detectado como no clasificables; esto es un ",round(100*num_noClasificables/num_nocat,2),"% de las prescripciones por texto libre.\n   SE PUEDEN CONSULTAR LAS PRESCRIPCIONES NO CLASIFICABLES DETECTADAS EN EL ARCHIVO ",nom_arc_noClasificables,"\n\n", 
  #     " * Un total de ", num_vac_fm, " se han detectado como VACUNAS O FÓRMULAS MAGISTRALES; esto es un ",round(100*num_vac_fm/num_nocat,2),"% de las prescripciones por texto libre.\n   SE PUEDEN CONSULTAR LAS PRESCRIPCIONES DE VACUNAS Y FÓRMULAS MAGISTRALES DETECTADAS EN EL ARCHIVO ",nom_arc_vac_fm,"\n\n", 
  #     " * Quedan ",num_nomed, " prescripciones"," (un ",round(100*num_nomed/num_nocat,2),"% de las de texto libre) ","que no son medicamentos y que hemos tratado de clasificar:\n",
  #     "     - Se han conseguido clasificar ", num_clasificados, ", lo que constituye un ",round(100*num_clasificados/num_nomed,2),"%, mientras que ", num_noclasificados, " prescripciones no han sido posible clasificarlas", " (",round(100*num_noclasificados/num_nomed,2),"%).\n       SE PUEDE CONSULTAR TODA LA INFORMACIÓN DE LAS PRESCRIPCIONES DE OTROS_PRODUCTOS EN ",nom_arc,"\n",
  #     "     - Se han detectado un total de ",num_nationalCode, " prescripciones de OTROS_PRODUCTOS que incorporaban su nationalCode; esto es un ",round(100*num_nationalCode/num_nomed,2),"% de las ", num_nomed," prescripciones que\n       hemos intentado clasificar. De entre estas se han clasificado ", num_nationalCode_clasificados, " (",round(100*num_nationalCode_clasificados/num_nationalCode,2),"%) y no se han conseguido clasificar ",  num_nationalCode_noclasificados, " (",round(100*num_nationalCode_noclasificados/num_nationalCode,2),"%).\n       SE PUEDEN CONSULTAR LAS PRESCRIPCIONES DE OTROS_PRODUCTOS CON NATIONAL CODE EN ",nom_arc_nationalCode,"\n",
  #     "     - Se han detectado un total de ",num_lab, " prescripciones de OTROS_PRODUCTOS que incorporaban información de su laboratorio (y no incorporaban nationalCode);\n       esto es un ",round(100*num_lab/num_nomed,2),"% de las ", num_nomed," prescripciones que hemos intentado clasificar. De entre estas se han clasificado ", num_lab_clasificados, " (",round(100*num_lab_clasificados/num_lab,2),"%) y no se han conseguido\n       clasificar ",  num_lab_noclasificados, " (",round(100*num_lab_noclasificados/num_lab,2),"%).\n       SE PUEDEN CONSULTAR LAS PRESCRIPCIONES DE OTROS_PRODUCTOS CON INFORMACIÓN DEL LABORATORIO EN ",nom_arc_lab,"\n",
  #     "SE PUEDE CONSULTAR LA FRECUENCIA DE LAS PRESCRIPCIONES NO CLASIFICADAS EN ",nom_frec,"\n",
  #     "SE PUEDE CONSULTAR LA INFORMACIÓN DE LA CLASIFICACIÓN DE TODAS LAS PRESCRIPCIONES PROPORCIONADAS EN ",nom,"\n",
  #     sep = "", file = paste0(salida,"/INFORME_",salida,".txt"))
  # 
  # # cat("\n\nPUEDE CONSULTAR EL INFORME EN EL FICHERO \033[1;31mINFORME_",salida,".txt\033[0m\n","OTROS DOCUMENTOS GENERADOS: \n \033[1;31mMEDICAMENTOS_",salida,".xlsx\033[0m\n \033[1;30mNO_CLASIFICABLES_",salida,".xlsx\033[0m\n \033[1;31mVACUNAS_FMAGISTRALES_",salida,".xlsx\033[0m\n \033[1;31mOTROS_PRODUCTOS_",salida,".xlsx\n\033[0m \033[1;30mNATIONALCODE_",salida,".xlsx\033[0m\n \033[1;30mLAB_",salida,".xlsx\n \033[1;31mCLASIFICACION_",salida,".xlsx\n \033[1;30mFRECUENCIA_NO_CLASIFICADOS_",salida,".xlsx\n \033[1;33mMETADATOS.txt\033[0m\n TODOS LOS FICHEROS SE ENCUENTRAN EN LA CARPETA \033[1;32m",salida,"\033[0m",sep = "")
  
  return(prescription_completo)
  }
  write.csv("Ninguna prescripcion realizada por texto libre","resultado_clasificacion.csv")
  
  }
  else {
    write.csv("Ninguna prescripcion realizada de OTROS PRODUCTOS","resultado_clasificacion.csv")
  }
}

# Leer tabla
df_prescription <- dbGetQuery(con_rempe, "
  SELECT *
  FROM rempe.prescription
  WHERE created_date >= '2025-04-11'
")


# Ejecutar clasificación
resultado <- clasifica(df_prescription)

# Guardar en nueva tabla
# dbWriteTable(
#   con_rempe,
#   name = Id(schema = "rempe", table = "prescription_aux"),
#   value = resultado,
#   append = TRUE
# )

dbDisconnect(con_rempe)
dbDisconnect(con_nomenclator)

