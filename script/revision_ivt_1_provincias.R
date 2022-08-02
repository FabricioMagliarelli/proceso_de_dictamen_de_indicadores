#Este Script descarga, compila y analiza las tablas del Indicador 1. El mismo trata
#la adscripcion de pacientes a los efectores que trabajan con el proyecto.
#Estas tablas cuentan con registros de personas, sus datos personales y datos de
#contacto. Como son datos sensibles, se quitaron del escript los links
#de referencia.
#En total se tenían más o menos 150.000 registros por Cuatrimestre.



# Librerías --------------------------------------------------------------------
# Primero cargamos las Librerías que se utilizaran a lo largo del script
#install.packages("usethis")
#install.packages("devtools")
#install.packages("glue")
#install.packages("rlang")
#install.packages("gargle")
#devtools::install_github("r-lib/gargle")
#install.packages("googlesheets4")
#install.packages("readxl")
library(usethis)
library(devtools)
library(googledrive)
library(googlesheets4)
library(tidyverse)
library(readxl)

# Descarga----------------------------------------------------------------------
#Se procede a descargar las bases del drive, se aprovecha aquí la librería
#googlesheets4 que nos permite interactuar con Googledrive directamente desde R.
#En total son 24 tablas, una por cada provincia (y CABA).

#hoja de links (esta tabla cuenta con los links de los 24 registros delas provincias)
while(TRUE){tabla_links <- try(
  read_sheet(ss = "...", 
             sheet="..."),T)
if(!is(tabla_links,'try-error')) break}

links<- tabla_links$links

#descargamos el CAI (Compromiso Anual de Gestión, se utiliza para revisiones mas adelante)----
while (TRUE) {cai_2021 <- try(
  read_sheet(ss = "...", 
             sheet="..."),T)
if(!is(cai_2021,'try-error')) break}
cai_2021$REFES<- as.character(cai_2021$REFES)

#nos quedamos solo con el indicador 1
cai_2021_ivt_1<- cai_2021[cai_2021$IVT == 1,]

#indiador 1----
#descarga de las tablas del indicador 1

#Para descargar las tablas se utilizará un loop while, anidado con un Try.
#de esta manera, el loop no continua hasta que la enésima tabla se descargue
#satisfactoriamente.

ivt_1<- list()

for(i in seq_along(links[-2])){while(TRUE){
  ivt_1[[i]]<- try(read_sheet(ss = links[-2][i],
                          sheet = "....",
                          range = "....!A5:AA",
                          col_names = F, 
                          col_types = "ccccDnccccccccccccccccccDcc"),T)
  if(!is(ivt_1[[i]],'try-error')) break}}


#guardamos backup RDS
saveRDS(ivt_1,"output/ivt_1.rds")

#definimos los nombres de columnas
n_col <- c("apellido_persona", "nombre_persona", "tipo_doc", "nro_doc", 
           "fecha_nac", "edad", "sexo", "genero", "localidad", 
           "calle", "numero", "lat_domic", "long_domic", "originaria", 
           "id_equipo", "sub_area", "micro_area", 
           "nombre_persona_equipo_contacto", 
           "apellido_persona_equipo_contacto", 
           "dni_persona_equipo_contacto", "caps_id", 
           "caps_nombre_proteger","provincia", "provincia_id", "fecha_contacto",
           "tipo_contacto","motivo_contacto")

ivt_1 <- map(ivt_1, set_names, n_col)

#consolidacion las tablas ----
ddjj_2021_03_ivt_1<- bind_rows(ivt_1)

#limpiamos la base
#sacamos los NA (filas vacias)
no_na<- !is.na(ddjj_2021_03_ivt_1$apellido_persona)
ddjj_2021_03_ivt_1<- ddjj_2021_03_ivt_1[no_na,]

#guardamos RDS y csv como buckup
saveRDS(ddjj_2021_03_ivt_1,"output/ddjj_2021_03_ivt_1.rds")
write.csv(ddjj_2021_03_ivt_1,"output/ddjj_2021_03_ivt_1.csv" )

#descarga sabana para verificar DDJJ anteriores.
#Esta sabana cuenta con la lista de efectores que ya aprobaron este indicador.
#Cada efector que presenta la provincia solo puede aprobar el indicador una sola
#vez
while(TRUE){aux_sabana<- try(
  read_sheet(ss = "...", 
             sheet="...",
             range = "...!A:M",
             col_types = "ccccccccnnncn"),T)
if(!is(aux_sabana,'try-error')) break}


#revision ----
#aqui se analizarán los datos y se realizara un dictamen del indicador.
#1. revisión si efector forma parte del Compromiso Anual de Gestión. 
revision_ivt_1<- ddjj_2021_03_ivt_1

revision_ivt_1<- revision_ivt_1 %>% 
  mutate(check_forma_parte_cai = case_when(
    caps_id %in% cai_2021_ivt_1$REFES ~ "si",
    TRUE ~ "no"
  )
  )

#2. revisión campos obligatorios completos
# revisamos que de lo cargado por las provincias no quede ningún campo 
# obligatorio vacío.  

revision_ivt_1 <- 
  revision_ivt_1 %>% 
  mutate(check_campos_oblig_vacios = apply(
    revision_ivt_1[,-c(16,17)], 1, function(x) sum(is.na(x))))

#3. revisión caso repetido
# Se verifica que no haya más de un registro por persona. 
# de encontrarse duplicados, se les deja una marca ("si") y se le reporta a las 
# provincias para que ella defina cual dejar declarado.
aux_dup <- (duplicated(revision_ivt_1$nro_doc))
aux_dup <- revision_ivt_1$nro_doc[aux_dup]

revision_ivt_1 <- 
  revision_ivt_1 %>% 
  mutate(check_es_persona_duplicada = case_when(
    nro_doc %in% aux_dup ~ "si",
    TRUE ~ "no"
  )
  )

#4.revisión si el efector ya aprobó en la DDJJ anterior
# se cruza con la sabana descargada anteriormente.
revision_ivt_1 <- 
  revision_ivt_1 %>% 
  mutate(check_efector_aprobo_anteriormente = case_when(
    caps_id %in% subset(aux_sabana,IVT == "1" & DICTAMEN == "positivo")$REFES ~ "si",
    TRUE ~ "no"
  )
  )


#5. fecha ultimos 12 meses
# La adscripción debe realizarse en los últimos 12 meses para que se valide el
# indicador

revision_ivt_1 <- 
  revision_ivt_1 %>% 
  mutate(check_persona_fecha_valida_contacto = case_when(
    fecha_contacto > "2020-12-31" ~ "si",
    TRUE ~ "no"
  )
  )

#6. revisión tipo de contacto
# El tipo de contacto detallado debe de ser alguno de los que se detallan en el
# vector acontinuación.
aux_tipo_contacto <- c("Contacto con la persona en el efector",
                       "Contacto con la persona en su domicilio",
                       "Actividad extramuros del equipo",
                       "Otra modalidad virtual/telefónica",
                       "Ronda Sanitaria",
                       "Telesalud")

revision_ivt_1 <- 
  revision_ivt_1 %>% 
  mutate(check_tipo_contacto_ok = case_when(
    tipo_contacto %in% aux_tipo_contacto ~ "si",
    TRUE ~ "no"
  )
  )

#7. revisión motivo de contacto da cuenta
# El motivo de contacto detallado debe de ser alguno de los que se detallan en el
# vector acontinuación.
aux_motivo_contacto <- c("Consulta y/o control médico",
                         "Consulta o control enfermería",
                         "Vacunación",
                         "Dispensa de medicamentos",
                         "Talleres/consejerías (actividad de prevención-promoción)",
                         "Búsqueda activa/recaptación",
                         "Consulta psicólogo/asistente social",
                         "Consulta odontológica",
                         "Consulta con otro profesional de la salud",
                         "Estudios /análisis",
                         "Detección y seguimiento COVID-19 de personas con ENT y/o factores de riesgo",
                         "Detección y seguimiento de personas con síntomas persistentes o secuelas por COVID-19")

revision_ivt_1 <- 
  revision_ivt_1 %>% 
  mutate(check_motivo_contacto_ok = case_when(
    motivo_contacto %in% aux_motivo_contacto ~ "si",
    TRUE ~ "no"
  )
  )

#8. Revisión con Bases de Salud Familiar
# entre la información detallada en los registros se encuentra
# el dato del profesional que realizo la adscripción. Ese profesional tiene que 
# ser parte de los equipos de salud validados por la 
# Dirección Nacional de Atención Primaria y Salud Comunitaria.
# Ellos nos pasan el listado de equipos para validar los cargados en los 
# Registros. Cada equipo tiene un código único cargado en el SISA.


#carga de bases salud familiar
while (TRUE) {df_sf<- try(read_sheet(ss = "...",
                   sheet = "...",
                   col_types = "cccccccc"),T)
if(!is(df_sf,'try-error')) break}

#revisión el id del equipo corresponde a los cargados en SISA
aux_id_equipos_sisa = unique(df_sf$`ID del Equipo`)
revision_ivt_1 <- 
  revision_ivt_1 %>% 
  mutate(check_id_equipo = case_when(
    id_equipo %in% aux_id_equipos_sisa ~ "si",
    TRUE ~ "no"
  )
  )

#dictamen final
#se dictamina caso por caso de acuerdo a lo analizado en los pasos anteriores.
revision_ivt_1 <- 
  revision_ivt_1 %>% 
  mutate(dictamen = case_when(
    revision_ivt_1$check_forma_parte_cai == "si" & 
      revision_ivt_1$check_campos_oblig_vacios == 0 &
      revision_ivt_1$check_es_persona_duplicada == "no" &
      revision_ivt_1$check_efector_aprobo_anteriormente == "no" &
      revision_ivt_1$check_persona_fecha_valida_contacto == "si" &
      revision_ivt_1$check_tipo_contacto_ok == "si" &
      revision_ivt_1$check_motivo_contacto_ok == "si" &
      revision_ivt_1$check_id_equipo == "si" ~ "positivo",
    TRUE ~ "negativo")
  )

#optenido el dictamen de cada adscripción, se realizan tablas que resumen la
#informacion para ser enviadas a las áreas sustantivas del proyecto.
#en base a esta información, se toman las decisiones correspondientes.

#division para la revisión y negativos por región
#Cada región tiene un consultor encargado. Los números que se ven ahí son los
#codigos INDEC de las distintas provincias.
#Dividimos la información por región para cada consultor cuente con los suyo
#para tomar las decisiones pertinentes.


NOA<- c("38","66","10","90","86")
NEA<- c("54","18","22","34")
CUYO<- c("46","50","70","74")
CEN<- c("6","82","14","30","2")
PATA<- c("42","58","62","26","78","94")

negativos_ivt_1_noa<- subset(revision_ivt_1,dictamen == "negativo" & provincia_id %in% NOA)
negativos_ivt_1_nea<- subset(revision_ivt_1,dictamen == "negativo" & provincia_id %in% NEA)
negativos_ivt_1_cuyo<- subset(revision_ivt_1,dictamen == "negativo" & provincia_id %in% CUYO)
negativos_ivt_1_cen<- subset(revision_ivt_1,dictamen == "negativo" & provincia_id %in% CEN)
negativos_ivt_1_pata<- subset(revision_ivt_1,dictamen == "negativo" & provincia_id %in% PATA)
negativos_ivt_1_na<- subset(revision_ivt_1,dictamen == "negativo" & is.na(provincia_id))

revision_ivt_1_noa<- subset(revision_ivt_1,provincia_id %in% NOA)
revision_ivt_1_nea<- subset(revision_ivt_1,provincia_id %in% NEA)
revision_ivt_1_cuyo<- subset(revision_ivt_1,provincia_id %in% CUYO)
revision_ivt_1_cen<- subset(revision_ivt_1,provincia_id %in% CEN)
revision_ivt_1_pata<- subset(revision_ivt_1,provincia_id %in% PATA)
revision_ivt_1_na<- subset(revision_ivt_1,is.na(provincia_id))

#Esta es una tabla adicional que cuenta con los registros duplicados.
#Esto se reporta a la provincia para que tome la decisión de cual quedarse

duplicados_ivt_1<- revision_ivt_1[revision_ivt_1$check_es_persona_duplicada=="si",]

# guardamos la revision backup
saveRDS(revision_ivt_1,"output/revision_ivt_1.rds")
write.csv(revision_ivt_1,"output/revision_ivt_1.csv",row.names = F)
write.csv(negativos_ivt_1_noa,"output/negativos_ivt_1_noa.csv",row.names = F)
write.csv(negativos_ivt_1_nea,"output/negativos_ivt_1_nea.csv",row.names = F)
write.csv(negativos_ivt_1_cuyo,"output/negativos_ivt_1_cuyo.csv",row.names = F)
write.csv(negativos_ivt_1_cen,"output/negativos_ivt_1_cen.csv",row.names = F)
write.csv(negativos_ivt_1_pata,"output/negativos_ivt_1_pata.csv",row.names = F)
write.csv(negativos_ivt_1_na,"output/negativos_ivt_1_na.csv",row.names = F)

write.csv(revision_ivt_1_noa,"output/revision_ivt_1_noa.csv",row.names = F)
write.csv(revision_ivt_1_nea,"output/revision_ivt_1_nea.csv",row.names = F)
write.csv(revision_ivt_1_cuyo,"output/revision_ivt_1_cuyo.csv",row.names = F)
write.csv(revision_ivt_1_cen,"output/revision_ivt_1_cen.csv",row.names = F)
write.csv(revision_ivt_1_pata,"output/revision_ivt_1_pata.csv",row.names = F)
write.csv(revision_ivt_1_na,"output/revision_ivt_1_na.csv",row.names = F)
write.csv(duplicados_ivt_1,"output/duplicados_ivt_1.csv",row.names = F)

# Dictamen consolidado
#Dado el gran caudal de datos, se prepara esta tabla resumen para presentar la 
#informacion. 

while(TRUE){metas_efectores <- try(
  read_sheet(ss = "...", 
             sheet = "...",
             range = "...!A:V",
             col_types = "ccccccccccccnnnnnccccc"),T)
if(!is(tabla_links,'try-error')) break}

dictamen_consolidado_ivt_1<- metas_efectores[,c(2:7,13,14,22)]

dictamen_consolidado_ivt_1<- dictamen_consolidado_ivt_1 %>%
  mutate("equipo nuclear" = "SI")

presentados<- count(revision_ivt_1,caps_id,name = "presentados")
positivos<- count(subset(revision_ivt_1,dictamen == "positivo"),caps_id,name = "aprobados")

dictamen_consolidado_ivt_1<- merge(x = dictamen_consolidado_ivt_1,
                                   y = presentados,
                                   by.x = "REFES",
                                   by.y = "caps_id")

dictamen_consolidado_ivt_1<- merge(x = dictamen_consolidado_ivt_1,
                                   y = positivos,
                                   by.x = "REFES",
                                   by.y = "caps_id",
                                   all.x = T )
dictamen_consolidado_ivt_1$aprobados[is.na(dictamen_consolidado_ivt_1$aprobados)]<- 0

dictamen_consolidado_ivt_1 <- dictamen_consolidado_ivt_1 %>%
  mutate("% aprobados" = round(aprobados/`Meta 3ra DDJJ 2021`,4))

dictamen_consolidado_ivt_1 <- dictamen_consolidado_ivt_1 %>%
  mutate("dictamen" = case_when(
    `% aprobados` >= 1 ~ "positivo",
    TRUE ~ "negativo"
  )
  )

colnames(dictamen_consolidado_ivt_1)<- c("REFES",
                                         "Nombre Efector	Jurisdicción",
                                         "Provincia",
                                         "Provincia_id",
                                         "Departamento",
                                         "Municipio",
                                         "Poblacion a Cargo (septiembre 2021)",
                                         "Meta (3ra DDJJ 2021)",
                                         "Area de responsabilidad digitalizada",
                                         "Equipo Nuclear conformado e inscrito en REFES",
                                         "Cantidad de población adscripta presentada en el marco del indicador",
                                         "Cantidad de población adscripta validada en el marco del indicador",
                                         "porc aprobados",
                                         "Cumple con adscripción del 30% de la población")

dictamen_consolidado_ivt_1<- dictamen_consolidado_ivt_1[order(as.numeric(dictamen_consolidado_ivt_1$Provincia_id)),]

write_sheet(data = dictamen_consolidado_ivt_1,
            ss = "...",
            sheet = "ivt_1")

#subimos revisiones al drive las tablas 
write_sheet(data = revision_ivt_1_cen,
            ss = "...",
            sheet = "ivt_1")

write_sheet(data = revision_ivt_1_cuyo,
            ss = "...",
            sheet = "ivt_1")

write_sheet(data = revision_ivt_1_na,
            ss = "...",
            sheet = "ivt_1")

write_sheet(data = revision_ivt_1_nea,
            ss = "...",
            sheet = "ivt_1")

write_sheet(data = revision_ivt_1_noa,
            ss = "...",
            sheet = "ivt_1")

write_sheet(data = revision_ivt_1_pata,
            ss = "...",
            sheet = "ivt_1")


# subimos negativos al drive
# subimos la información a unos archivos en el drive. 
# se crea un archivo por región y dentro del mismo, una solapa por provincia.

for (i in unique(revision_ivt_1_cen$provincia)) {
  write_sheet(data = subset(negativos_ivt_1_cen,provincia == i),
              ss = "...",
              sheet = i)
}

for (i in unique(revision_ivt_1_cuyo$provincia)) {
  write_sheet(data = subset(negativos_ivt_1_cuyo,provincia == i),
              ss = "...",
              sheet = i)
}

for (i in unique(revision_ivt_1_nea$provincia)) {
  write_sheet(data = subset(negativos_ivt_1_nea,provincia == i),
              ss = "...",
              sheet = i)
}

for (i in unique(revision_ivt_1_noa$provincia)) {
  write_sheet(data = subset(negativos_ivt_1_noa,provincia == i),
              ss = "...",
              sheet = i)
}

for (i in unique(revision_ivt_1_pata$provincia)) {
  write_sheet(data = subset(negativos_ivt_1_pata,provincia == i),
              ss = "...",
              sheet = i)
}

write_sheet(data = negativos_ivt_1_na,
            ss = "...",
            sheet = "NA")

