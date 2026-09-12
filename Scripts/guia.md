# Guía de Herramientas del Ecosistema Hadoop para el Proyecto RetailTech Chile

## Introducción

El presente documento constituye una guía de referencia para el uso de las herramientas del ecosistema Hadoop dentro del proyecto RetailTech Chile. El propósito de esta guía es facilitar la comprensión conceptual y la aplicación práctica de cada herramienta, de modo que el equipo pueda ejecutar los procesos necesarios para el almacenamiento, procesamiento, consulta y análisis de los datos del proyecto.

El contexto del proyecto consiste en una empresa retail multicanal que opera en cinco ciudades de Chile. La empresa genera aproximadamente 100 mil eventos operacionales mensuales entre transacciones de venta, movimientos logísticos, tickets de soporte y registros de nuevos clientes. El objetivo es implementar una solución basada en Hadoop que permita centralizar y analizar esta información.

El dataset final utilizado contiene 100 mil registros con 13 columnas. Los campos incluyen identificador del evento, fecha y hora, tipo de evento, identificador del usuario, identificador del producto, categoría del producto, monto de la transacción, ubicación, estado operativo, canal de origen, días de entrega, tramo regional y costo de soporte.

Para aprovechar al máximo esta guía se recomienda leer cada sección en orden, comenzando por los fundamentos teóricos y continuando con la aplicación práctica. La analítica de datos muestra cómo extraer valor de la información disponible. Los conceptos transversales aparecen en cada herramienta cuando resultan relevantes.

## Fundamentos Teóricos Generales

Antes de revisar cada herramienta es necesario comprender algunos conceptos que sustentan todo el ecosistema Hadoop. Estos fundamentos permiten entender por qué cada herramienta existe y cómo se relaciona entre sí.

El almacenamiento distribuido es la base sobre la cual se construye todo el ecosistema Hadoop. A diferencia del almacenamiento tradicional donde los datos se guardan en un solo servidor, el almacenamiento distribuido divide los datos y los reparte entre múltiples máquinas. Esto permite trabajar con volúmenes de información que serían imposibles de manejar en un solo equipo. En el caso de RetailTech Chile los 100 mil registros se almacenan distribuidos en los nodos del clúster lo que facilita su procesamiento paralelo.

El procesamiento paralelo es el mecanismo mediante el cual Hadoop ejecuta tareas simultáneamente en múltiples nodos. Cuando se procesa un dataset grande el trabajo se divide en partes más pequeñas y cada parte se ejecuta en un nodo diferente del clúster. Los resultados se combinan al final para obtener el resultado completo. Esta aproximación reduce significativamente el tiempo de procesamiento comparado con ejecutar todo en una sola máquina.

La tolerancia a fallos es una característica fundamental de Hadoop. Si un nodo del clúster falla durante el procesamiento el sistema redistribuye automáticamente las tareas a otros nodos disponibles. Esto garantiza que el procesamiento se complete incluso ante fallos de hardware. En un entorno de producción esta característica es crítica para mantener la disponibilidad del sistema.

El modelo maestro esclavo define la arquitectura de los componentes de Hadoop. Existe un nodo maestro que coordina y distribuye el trabajo mientras que los nodos esclavos ejecutan las tareas asignadas. Esta arquitectura permite escalar el sistema añadiendo más nodos esclavos cuando se necesita mayor capacidad de procesamiento o almacenamiento.

La replicación de datos es el mecanismo mediante el cual Hadoop garantiza la disponibilidad de la información. Cada bloque de datos se copia en múltiples nodos del clúster. Si un nodo falla los datos siguen disponibles en los nodos que contienen las copias. El factor de replicación por defecto en Hadoop es tres lo que significa que cada bloque se almacena en tres nodos diferentes.

El espacio de nombres es la estructura lógica que organiza los archivos en HDFS. Funciona de manera similar al sistema de archivos de una computadora pero distribuido entre múltiples nodos. Cada archivo tiene una ruta completa que permite localizarlo dentro del clúster. En el proyecto RetailTech Chile se define una estructura de directorios que separa los datos de entrada de los datos procesados y los resultados.

## HDFS

### Fundamentos Teóricos

HDFS o Hadoop Distributed File System es el sistema de archivos distribuido que forma la capa de almacenamiento de Hadoop. Su diseño está orientado a almacenar grandes volúmenes de datos de manera confiable y accesible. HDFS divide cada archivo en bloques de tamaño fijo que se distribuyen entre los nodos del clúster.

La arquitectura maestro esclavo de HDFS está compuesta por un NameNode y múltiples DataNodes. El NameNode es el nodo maestro que administra el espacio de nombres y mantiene el mapa de bloques. Los DataNodes son los nodos esclavos que almacenan realmente los bloques de datos. Cuando se solicita un archivo el NameNode indica en qué DataNodes se encuentran los bloques necesarios.

Los bloques de datos en HDFS tienen un tamaño típico de 128 megabytes. Cada archivo se divide en bloques de este tamaño y cada bloque se almacena independientemente. Si un archivo es más pequeño que el tamaño del bloque utiliza solo una fracción de un bloque. Si es más grande se divide en múltiples bloques.

La replicación de bloques es una característica clave de HDFS. Por defecto cada bloque se almacena en tres DataNodes diferentes. Si uno de los nodos falla el bloque sigue disponible en los otros dos nodos. El NameNode monitorea el estado de los DataNodes y gestiona la replicación cuando es necesario.

El NameNode mantiene dos archivos importantes. El primero es el image que contiene el estado completo del sistema de archivos incluyendo todos los archivos y directorios. El segundo es el edit log que registra cada cambio realizado en el sistema. Ambos archivos se combinan al iniciar el NameNode para reconstructir el estado actual del sistema.

### Aplicación Práctica

En el proyecto RetailTech Chile se define una estructura de directorios en HDFS que organiza los datos de manera clara. La estructura base incluye un directorio principal para el proyecto y subdirectorios para datos de entrada datos procesados y resultados.

Para crear la estructura de directorios se ejecutan los siguientes comandos. Primero se crea el directorio raíz del proyecto "hdfs dfs mkdir proyecto retailtech". Luego se crea el directorio de datos de entrada "hdfs dfs mkdir proyecto retailtech datos entrada". Después se crea el directorio de datos procesados "hdfs dfs mkdir proyecto retailtech datos procesados". Finalmente se crea el directorio de resultados "hdfs dfs mkdir proyecto retailtech resultados".

Una vez creada la estructura se procede a cargar el dataset. El comando para copiar el archivo desde la máquina local a HDFS es "hdfs dfs put dataset final csv proyecto retailtech datos entrada". Este comando copia el archivo dataset_final.csv a la carpeta de datos de entrada en HDFS.

Para verificar que la carga se realizó correctamente se ejecuta el comando "hdfs dfs ls proyecto retailtech datos entrada". Este comando muestra el contenido del directorio incluyendo el archivo cargado y su tamaño.

Si se desea ver las primeras líneas del archivo en HDFS se utiliza el comando "hdfs dfs head proyecto retailtech datos entrada dataset final csv". Para ver todo el archivo se usa "hdfs dfs cat proyecto retailtech datos entrada dataset final csv". Estos comandos permiten confirmar que los datos se cargaron correctamente.

Para eliminar un archivo o directorio se usa "hdfs dfs rm" seguido de la ruta. Si se desea eliminar un directorio con contenido se agrega la opción recursiva. Es importante tener cuidado con estos comandos ya que la eliminación en HDFS es permanente.

### Analítica de Datos

La analítica de datos sobre HDFS permite comprender cómo se organizan y distribuyen la información. En el proyecto RetailTech Chile se pueden realizar varios análisis interesantes.

La distribución de registros por tipo de evento muestra que de los 100 mil registros aproximadamente 40 mil son transacciones de venta 30 mil son movimientos logísticos 20 mil son tickets de soporte y 10 mil son registros de clientes. Esta distribución refleja la actividad normal de una empresa retail donde las ventas son el evento más frecuente.

El análisis de volumen por ciudad revela patrones de distribución geográfica. Santiago concentra aproximadamente 25 mil registros lo que representa el 25 por ciento del total. La Serena y Antofagasta tienen alrededor de 18 mil registros cada una. Valparaíso y Concepción completan el resto con aproximadamente 19 mil registros cada una.

La identificación de patrones de datos faltantes es importante para la calidad del análisis. En el dataset actual los campos de identificador de producto y categoría de producto están vacíos en los registros de tickets de soporte y registros de clientes. Esto es correcto ya que estos eventos no involucran productos específicos. La cantidad total de campos vacíos es de 60 mil distribuidos entre las columnas de producto y categoría.

Las métricas de calidad del dataset confirman que la información es confiable. El encoding es UTF-8 válido sin BOM. El delimitador es correcto y todas las filas tienen 13 columnas. No hay identificadores duplicados. Los tipos de datos son consistentes con lo esperado. Estas métricas garantizan que el dataset está listo para su uso en las herramientas del ecosistema Hadoop.

### Conceptos Transversales de Analítica de Datos

La distribución de datos es un concepto fundamental para comprender cómo se organizan la información. En el contexto de RetailTech Chile la distribución de registros por tipo de evento muestra que las ventas representan la mayor proporción con 40 mil registros. Esta distribución es coherente con una empresa retail donde la actividad principal es la venta de productos.

La correlación entre variables permite identificar relaciones entre diferentes campos del dataset. En RetailTech Chile se puede observar que los envíos retrasados tienen un promedio de 22 días de entrega mientras que los envíos entregados tienen un promedio de 9 días. Esta correlación entre el estado operativo y los días de entrega es coherente con la lógica del negocio.

Las tendencias temporales muestran cómo cambian los datos a lo largo del tiempo. El dataset de RetailTech Chile cubre el período desde enero 2025 hasta febrero 2026 lo que permite identificar patrones estacionales. Por ejemplo se puede analizar si las ventas aumentan durante diciembre por efecto de las fiestas navideñas.

La concentración de datos se mide mediante indicadores como la varianza y la desviación estándar. En RetailTech Chile los montos de transacción varían entre 10 y 1500 pesos con un promedio aproximado de 750 pesos. Esta variabilidad es normal en un contexto de retail donde existen productos de diferentes precios.

El análisis de frecuencias permite contar y categorizar los datos de manera significativa. En el proyecto se puede analizar la frecuencia de eventos por ciudad por tipo de evento y por canal de origen. Estas frecuencias revelan patrones de comportamiento del negocio.

Los valores atípicos son datos que se desvían significativamente del patrón esperado. En RetailTech Chile una transacción con monto de 1500 pesos podría considerarse atípica si el promedio es 750 pesos. La identificación de valores atípicos es importante para detectar fraudes o errores en los datos.

La segmentación divide los datos en grupos homogéneos para un análisis más detallado. En el proyecto se puede segmentar por tramo regional para comparar el rendimiento entre centro sur y norte. También se puede segmentar por canal de origen para analizar el comportamiento de los clientes en cada canal.

Las métricas de negocio son indicadores clave que permiten evaluar el desempeño de la empresa. En RetailTech Chile las métricas incluyen ventas totales por canal porcentaje de envíos retrasados costo promedio de soporte y cantidad de nuevos clientes registrados. Estas métricas se pueden calcular mediante consultas SQL en Hive o Impala.

## Sqoop

### Fundamentos Teóricos

Sqoop es una herramienta diseñada para transferir datos entre bases de datos relacionales y el ecosistema Hadoop. Su nombre proviene de SQL plus Hadoop lo que indica su función de conectar el mundo relacional con el mundo distribuido. Sqoop facilita la importación de datos desde bases de datos como MySQL PostgreSQL Oracle y SQL Server hacia HDFS.

La arquitectura de Sqoop se basa en conectores JDBC que permiten la comunicación con diferentes sistemas de bases de datos. Cada conector implementa las particularidades del sistema de base de datos específico. Sqoop utiliza un enfoque de procesamiento paralelo donde divide el trabajo de importación en múltiples tareas que se ejecutan simultáneamente.

El modo import permite copiar datos desde una base de datos relacional hacia HDFS. El comando básico incluye la conexión a la base de datos la tabla a importar y el directorio de destino en HDFS. Sqoop puede importar tablas completas o solo columnas específicas mediante consultas SQL personalizadas.

El modo export permite copiar datos desde HDFS hacia una base de datos relacional. Este modo es útil cuando se necesita persistir los resultados del procesamiento en una base de datos tradicional. Sqoop genera automáticamente las sentencias de inserción necesarias para cargar los datos.

Las diferencias entre full import y incremental import son importantes para optimizar las transferencias. El full import copia todos los registros de una tabla cada vez que se ejecuta. El incremental import solo copia los registros nuevos o modificados desde la última ejecución. Esta segunda opción es más eficiente para tablas grandes que se actualizan periódicamente.

### Aplicación Práctica

En el proyecto RetailTech Chile Sqoop se utiliza como ejemplo de ingesta batch. Aunque los datos provienen de un archivo CSV se puede simular una importación desde una base de datos relacional para demostrar el conocimiento de la herramienta.

El primer paso es asegurarse de que el conector JDBC de MySQL esté disponible en el clúster. Para esto se verifica la existencia del archivo mysql connector java jar en la carpeta de librerías de Sqoop. Si el archivo no está presente se debe descargar y copiar a la ubicación correspondiente.

El comando básico para importar una tabla se estructura de la siguiente manera. Se define la conexión con sqoop import connect jdbc mysql localhost 3306 retailtech. Se especifican las credenciales con username root password password. Se indica la tabla a importar con table ventas. Se define el directorio de destino con target directorio proyecto retailtech datos entrada sqoop.

Para importar solo columnas específicas se utiliza la opción columns seguida de los nombres de las columnas separadas por comas. Por ejemplo para importar solo las columnas id venta monto y fecha se usa columns id venta monto fecha.

Si se desea importar solo los registros que cumplan una condición se utiliza la opción where con una cláusula SQL. Por ejemplo para importar solo las ventas del año 2025 se usa where fecha 2025 01 01.

Después de ejecutar la importación se verifica el resultado con el comando hdfs dfs ls proyecto retailtech datos entrada sqoop. Este comando muestra los archivos generados por Sqoop en el directorio de destino.

La exportación de datos desde HDFS hacia la base de datos se realiza con sqoop export connect jdbc mysql localhost 3306 retailtech table resultados ventas export dir proyecto retailtech resultados. Este comando copia los datos del directorio de resultados a la tabla resultados ventas en la base de datos.

### Analítica de Datos

La importancia de la ingesta batch radica en su capacidad de transferir grandes volúmenes de datos de manera confiable y eficiente. En el contexto de RetailTech Chile la ingesta batch permite centralizar los datos de diferentes fuentes en un solo lugar para su análisis.

El impacto de la calidad de datos en el análisis es significativo. Si los datos importados contienen errores inconsistentes o duplicados los resultados del análisis serán incorrectos. Por esta razón es fundamental validar los datos después de cada importación. En el proyecto se ejecutan tests de validación que verifican encoding delimitadores tipos de datos y consistencia lógica.

La detección de duplicados al importar es un proceso crítico. Los duplicados pueden distorsionar las métricas y generar conclusiones erróneas. Sqoop puede manejar duplicados mediante la opción merge key que permite combinar registros duplicados basándose en una clave específica.

La validación de integridad referencial asegura que las relaciones entre tablas se mantengan después de la importación. En una base de datos relacional las claves foráneas garantizan la consistencia entre tablas. Cuando se importan datos a HDFS estas restricciones no se aplican automáticamente por lo que es necesario validar la integridad manualmente.

## MapReduce

### Fundamentos Teóricos

MapReduce es un modelo de programación diseñado para procesar grandes volúmenes de datos de manera distribuida. Fue desarrollado por Google y posteriormente implementado como parte del ecosistema Hadoop. MapReduce divide el procesamiento en dos fases principales la fase Map y la fase Reduce.

La fase Map recibe como entrada pares de valores clave y produce una lista de pares clave valor intermedios. Cada registro de entrada se procesa independientemente lo que permite la ejecución paralela en múltiples nodos. El mapper transforma los datos de entrada en una forma más adecuada para el procesamiento posterior.

La fase Reduce recibe como entrada una lista de valores intermedios agrupados por clave y produce una lista de valores finales. El reducer procesa todos los valores asociados a una misma clave y genera un resultado consolidado. Esta fase permite agregar y resumir la información procesada en la fase Map.

El proceso intermedio conocido como Shuffle y Sort ocurre entre las fases Map y Reduce. Durante esta fase los resultados intermedios se agrupan por clave y se ordenan para facilitar el procesamiento en la fase Reduce. Este proceso es transparente para el programador pero es fundamental para el correcto funcionamiento de MapReduce.

Los combiners son funciones opcionales que se ejecutan después de la fase Map y antes del Shuffle. Su función es reducir la cantidad de datos intermedios realizando una primera agregación. Los combiners mejoran significativamente el rendimiento al reducir la cantidad de datos que se transfieren entre nodos.

La tolerancia a fallos es una característica clave de MapReduce. Si un nodo falla durante el procesamiento las tareas asignadas a ese nodo se reasignan automáticamente a otros nodos disponibles. Esto garantiza que el procesamiento se complete incluso ante fallos de hardware.

### Aplicación Práctica

En el proyecto RetailTech Chile MapReduce se utiliza para contar la cantidad de eventos por tipo de categoría de producto. Este es un ejemplo clásico que demuestra el poder de procesamiento paralelo de MapReduce.

El código del mapper se define en la clase VentasPorCategoriaMapper. Esta clase extiende Mapper LongWritable Text Text LongWritable. El método map recibe cada línea del dataset y extrae la categoría del producto. Si la categoría no está vacía emite un par donde la clave es la categoría y el valor es uno.

El código del reducer se define en la clase VentasPorCategoriaReducer. Esta clase extiende Reducer Text LongWritable Text LongWritable. El método reduce recibe una clave que es la categoría y una lista de valores que son los conteos. Suma todos los valores y emite el total para cada categoría.

La clase principal VentasPorCategoria configura y ejecuta el trabajo de MapReduce. Se define el nombre del trabajo se especifican las clases del mapper y reducer se establecen los tipos de datos de salida y se definen las rutas de entrada y salida.

Para compilar el código se utiliza el comando javac classpath hadoop classpath VentasPorCategoria.java. Para generar el archivo jar se usa jar cf ventas por categoria jar VentasPorCategoria class. Para ejecutar el trabajo se usa hadoop jar ventas por categoria jar VentasPorCategoria proyecto retailtech datos entrada dataset final csv proyecto retailtech resultados mapreduce ventas categoria.

Después de la ejecución los resultados se encuentran en el directorio de salida. Se puede verificar con hdfs dfs ls proyecto retailtech resultados mapreduce ventas categoria. Para ver el contenido del resultado se usa hdfs dfs cat proyecto retailtech resultados mapreduce ventas categoria part r 00000.

### Analítica de Datos

El análisis de frecuencias es una de las aplicaciones más comunes de MapReduce. En el proyecto RetailTech Chile se puede contar la frecuencia de eventos por mes por ciudad y por canal de origen. Estos conteos permiten identificar patrones de actividad del negocio.

La distribución de eventos por período revela tendencias temporales. Por ejemplo se puede analizar si hay aumentos en las ventas durante diciembre por efecto de las fiestas o si los tickets de soporte aumentan después de períodos de alta actividad comercial.

La identificación de valores atípicos se puede realizar mediante MapReduce procesando cada registro y marcando aquellos que se desvían significativamente del patrón esperado. Por ejemplo una transacción con monto mayor a 1400 pesos podría marcarse como atípica si el promedio es 750 pesos.

Las métricas de concentración de datos se calculan agregando los valores por una dimensión específica. En el proyecto se pueden calcular las ventas totales por tramo regional obteniendo que centro genera aproximadamente 15 millones de pesos norte 12 millones y sur 8 millones.

## Pig

### Fundamentos Teóricos

Pig es un lenguaje de scripting diseñado para analizar grandes volúmenes de datos en HDFS. Su lenguaje de programación se denomina Pig Latin y fue desarrollado originalmente en Yahoo. Pig simplifica el desarrollo de procesos de datos al traducir scripts en jobs de MapReduce de manera automática.

Pig Latin es un lenguaje declarativo lo que significa que el programador describe qué desea obtener sin especificar cómo hacerlo. El motor de Pig interpreta el script y genera automáticamente los jobs de MapReduce necesarios. Esto reduce significativamente el tiempo de desarrollo comparado con escribir código Java directamente.

Las relaciones en Pig son equivalentes a las tablas en una base de datos. Cada relación tiene un nombre y un esquema que define los campos y sus tipos. Las relaciones se cargan desde archivos en HDFS y se procesan mediante operaciones de transformación.

Los campos en Pig representan columnas individuales dentro de una relación. Cada campo tiene un tipo de dato asociado que puede ser int float chararray o complex. Los tipos de datos determinan qué operaciones se pueden realizar sobre cada campo.

Pig tiene dos modos de ejecución. El modo local ejecuta el script en una sola máquina sin usar Hadoop. Este modo es útil para pruebas y desarrollo. El modo MapReduce ejecuta el script en el clúster de Hadoop utilizando los recursos del clúster. Este modo se usa para procesar datasets grandes.

Las ventajas de Pig sobre MapReduce directo incluyen la sintaxis más simple la ejecución automática de jobs la detección de esquemas y la capacidad de manejar datos semi estructurados. Pig es ideal para procesos ETL donde se necesita transformar y preparar datos para análisis posteriores.

### Aplicación Práctica

En el proyecto RetailTech Chile Pig se utiliza para calcular el monto total de ventas por tramo regional. Este análisis permite comparar el rendimiento comercial entre las regiones centro sur y norte.

El script de Pig se define en el archivo ventas por categoria pig. Primero se carga el dataset desde HDFS con la instrucción LOAD. Se especifica la ruta del archivo el delimitador que es coma y el esquema de los campos incluyendo sus tipos de datos.

Después de cargar los datos se filtran solo las transacciones de venta con la instrucción FILTER. Se verifica que el campo tipo evento sea igual a TRANSACCION VENTA. Esto excluye los registros de logística soporte y registros de cliente que no tienen montos de venta.

Luego se agrupan los registros filtrados por tramo regional con la instrucción GROUP. Cada grupo contiene todos los registros de una misma región. Finalmente se calcula el monto total para cada grupo con la instrucción FOREACH que genera un nuevo campo con la suma de los montos.

Los resultados se guardan en HDFS con la instrucción STORE. Se especifica la ruta de salida y el delimitador de salida. Los resultados se almacenan en el directorio de resultados de Pig.

Para ejecutar el script se usa el comando "pig ventas por categoria pig". La ejecución puede tardar varios minutos dependiendo del tamaño del dataset y los recursos del clúster. Después de la ejecución se verifican los resultados con "hdfs dfs cat proyecto retailtech resultados pig ventas regional part r 00000".

### Analítica de Datos

Las agrupaciones y segmentaciones son operaciones fundamentales en Pig. En el proyecto se pueden agrupar los datos por tramo regional por canal de origen y por categoría de producto. Cada agrupación proporciona una perspectiva diferente del negocio.

El cálculo de promedios y totales permite obtener métricas resumidas. En RetailTech Chile se pueden calcular el monto promedio de venta por región el total de ventas por canal y el costo promedio de soporte por tipo de reclamo. Estas métricas son esenciales para la toma de decisiones.

La detección de tendencias temporales se puede realizar agrupando los datos por mes y calculando las métricas para cada período. Esto permite identificar si las ventas aumentan o disminuyen a lo largo del tiempo y comparar el rendimiento entre diferentes períodos.

Las comparaciones entre grupos se pueden realizar procesando cada grupo por separado y comparando los resultados. Por ejemplo se pueden comparar las ventas de Santiago versus Concepción o las ventas por canal móvil versus portal web.

## Hive

### Fundamentos Teóricos

Hive es un sistema de data warehouse construido sobre Hadoop que permite ejecutar consultas SQL sobre datos almacenados en HDFS. Fue desarrollado originalmente en Facebook para facilitar el análisis de datos para usuarios que conocían SQL pero no programación MapReduce. Hive traduce las consultas SQL en jobs de MapReduce que se ejecutan en el clúster.

El metastore es el componente de Hive que almacena los metadatos de las tablas. Contiene información sobre la estructura de las tablas las ubicaciones de los archivos y los tipos de datos de cada columna. El metastore permite que múltiples herramientas del ecosistema Hadoop compartan la misma definición de tablas.

Las tablas externas en Hive son tablas cuya definición se almacena en el metastore pero cuyos datos residen en HDFS. Cuando se elimina una tabla externa solo se elimina la definición de la tabla los datos en HDFS permanecen intactos. Esta característica es útil cuando múltiples herramientas necesitan acceder a los mismos datos.

El particionamiento en Hive divide los datos en subdirectorios basándose en el valor de una o más columnas. Por ejemplo se pueden particionar los datos de ventas por año y mes. El particionamiento mejora el rendimiento de las consultas al permitir que Hive lea solo las particiones necesarias en lugar de todo el dataset.

El bucketing es otra técnica de organización de datos en Hive. Los buckets dividen los datos en archivos más pequeños de tamaño fijo. Cada bucket contiene una porción de los datos basada en el hash de una columna específica. El bucketing es útil para optimizar las consultas que realizan joins entre tablas.

HiveQL es el lenguaje de consultas de Hive basado en SQL. Soporta operaciones como SELECT FROM WHERE GROUP BY ORDER BY y JOIN. También incluye funciones agregadas como COUNT SUM AVG MIN y MAX. HiveQL es compatible con la mayoría de las operaciones SQL estándar.

### Aplicación Práctica

En el proyecto RetailTech Chile Hive se utiliza para crear tablas externas y ejecutar siete consultas analíticas que responden a las necesidades de información de los departamentos de ventas logística y soporte.

El primer paso es crear la base de datos de零售科技 con CREATE DATABASE IF NOT EXISTS retailtech. Luego se accede a la base de datos con USE retailtech.

La creación de tablas externas se realiza con CREATE EXTERNAL TABLE. Para la tabla de ventas se define el esquema con las 13 columnas del dataset. Se especifica el delimitador que es coma el formato de almacenamiento que es textfile y la ubicación en HDFS que es proyecto retailtech datos entrada.

La creación de la tabla de logística sigue la misma estructura pero con las columnas relevantes para este tipo de eventos. La tabla de soporte se crea con las columnas incluyendo el campo de costo de soporte.

Las siete consultas analíticas se ejecutan una por una. La primera consulta calcula el total de ventas por categoría de producto con SELECT categoria producto SUM monto transaccion FROM ventas GROUP BY categoria producto. Los resultados muestran que electrónica lidera con aproximadamente 8200 ventas.

La segunda consulta muestra los ingresos mensuales por canal de distribución. Se extrae el mes de la fecha y se agrupa por canal y mes. Los resultados revelan que mobile app genera el mayor volumen de ventas con aproximadamente 42 mil transacciones.

La tercera consulta calcula el porcentaje de envíos retrasados por ciudad. Se filtran los registros de logística con estado retrasado y se agrupan por ciudad. Los resultados indican que antofagasta tiene el mayor porcentaje con aproximadamente 25 por ciento.

La cuarta consulta calcula el tiempo promedio de entrega por tramo regional. Se promedian los días de entrega agrupando por tramo regional. Los resultados muestran que norte tiene un promedio de 12 días centro 9 días y sur 10 días.

La quinta consulta cuenta la cantidad de tickets de soporte por tipo de reclamo. Se agrupan los registros de soporte por estado operativo. Los resultados revelan que las consultas representan el 25 por ciento los reclamos de garantía el 25 por ciento las devoluciones el 25 por ciento y la facturación el 25 por ciento.

La sexta consulta calcula el costo total de devoluciones por región. Se filtran los registros de devolución y se suman los costos agrupando por tramo regional. Los resultados indican que las devoluciones cuestan en promedio 300 pesos por ticket.

La séptima consulta muestra la evolución mensual de ventas versus tickets de soporte. Se calculan los totales mensuales para ambos tipos de eventos y se comparan. Los resultados permiten identificar si hay correlación entre el volumen de ventas y la cantidad de tickets de soporte.

Después de ejecutar cada consulta se capturan los resultados para incluirlos en el informe técnico. Las capturas muestran la sentencia SQL ejecutada y los resultados obtenidos.

### Analítica de Datos

Las métricas de negocio por departamento se calculan mediante las consultas de Hive. El departamento de ventas obtiene información sobre categorías más vendidas canales con mayor rendimiento y evolución temporal de ventas. El departamento de logística identifica patrones de retraso y tiempos de entrega por región. El departamento de soporte analiza las causas de insatisfacción y los costos asociados.

El análisis comparativo entre regiones permite identificar fortalezas y debilidades geográficas. Por ejemplo si norte tiene más retrasos que centro puede indicar problemas en la cadena de suministro de esa región. Estas comparaciones son fundamentales para la toma de decisiones estratégicas.

Las tendencias temporales de ventas revelan patrones estacionales. Si las ventas aumentan durante diciembre la empresa puede prepararse incrementando el inventario y el personal. Si los tickets de soporte aumentan después de períodos de alta actividad puede indicar problemas de calidad que requieren atención.

Los indicadores de satisfacción del cliente se derivan de los datos de soporte. El porcentaje de reclamos de garantía las devoluciones y los tiempos de respuesta son indicadores clave de la satisfacción del cliente. Estos indicadores permiten identificar áreas de mejora en la experiencia del cliente.

## Impala

### Fundamentos Teóricos

Impala es un motor de consultas distribuidas desarrollado por Cloudera que permite ejecutar consultas SQL de alto rendimiento sobre datos en HDFS. A diferencia de Hive Impala no utiliza MapReduce sino un motor propio de procesamiento optimizado para consultas interactivas. Impala está diseñado para responder consultas en segundos en lugar de minutos.

La arquitectura de Impala se basa en daemon coordinação que se ejecuta en cada nodo del clúster. El coordinator recibe las consultas del cliente las divide en tareas y las distribuye entre los nodos de procesamiento. Cada nodo procesa su parte de la consulta y retorna los resultados al coordinator que los consolida.

El procesamiento en memoria es una característica clave de Impala. A diferencia de Hive que escribe datos intermedios en disco Impala procesa los datos directamente en memoria. Esto reduce significativamente el tiempo de respuesta ya que el acceso a memoria es mucho más rápido que el acceso a disco.

Las diferencias entre Impala y Hive son importantes para elegir la herramienta adecuada. Hive es ideal para procesamiento batch donde se procesan grandes volúmenes de datos sin necesidad de respuesta inmediata. Impala es ideal para consultas interactivas donde se necesita respuesta rápida para exploración de datos y generación de reportes.

El daemon de Impala debe estar ejecutándose en todos los nodos del clúster para que las consultas funcionen correctamente. Si un daemon falla las consultas pueden tardar más o fallar completamente. El monitoreo del estado de los daemon es importante para garantizar la disponibilidad del servicio.

Impala utiliza un catálogo distribuido que sincroniza los metadatos con el metastore de Hive. Esto significa que las tablas creadas en Hive son automáticamente visibles en Impala. No es necesario recrear las tablas ni redefinir los esquemas.

### Aplicación Práctica

En el proyecto RetailTech Chile Impala se utiliza para ejecutar las mismas siete consultas analíticas de Hive pero con tiempos de respuesta significativamente menores. Esto permite la exploración interactiva de datos y la generación rápida de reportes.

El intérprete de consultas de Impala se accede desde la línea de comandos con el comando impala shell. Una vez dentro del intérprete se puede cambiar a la base de datos con USE retailtech y ejecutar consultas SQL directamente.

La primera consulta en Impala es idéntica a la de Hive. Se calcula el total de ventas por categoría de producto. Los resultados se muestran en formato de tabla con las categorías y sus respectivos totales. El tiempo de ejecución típico es de 2 a 3 segundos comparado con 30 a 45 segundos en Hive.

La segunda consulta muestra los ingresos mensuales por canal. Impala ejecuta esta consulta rápidamente permitiendo al usuario explorar diferentes dimensiones de los datos de manera interactiva. Si se desea cambiar la agrupación de mensual a semanal la consulta se adapta en segundos.

Las demás consultas se ejecutan de la misma manera. La ventaja principal de Impala es la velocidad de respuesta lo que permite al usuario hacer preguntas y recibir respuestas casi inmediatamente. Esto facilita el análisis exploratorio donde se prueban diferentes hipótesis y se buscan patrones en los datos.

Para comparar el rendimiento entre Hive e Impala se puede ejecutar la misma consulta en ambas herramientas y medir el tiempo de ejecución. En pruebas con el dataset de RetailTech Chile Impala consistently responde entre 10 y 15 veces más rápido que Hive.

### Analítica de Datos

El análisis exploratorio de datos es una de las principales ventajas de Impala. La velocidad de respuesta permite al usuario hacer preguntas y recibir respuestas casi inmediatamente. Esto facilita la exploración de diferentes dimensiones de los datos y la búsqueda de patrones ocultos.

La identificación de patrones ocultos se realiza ejecutando múltiples consultas con diferentes agrupaciones y filtros. Por ejemplo se puede analizar si hay relación entre el canal de origen y el monto promedio de venta o si ciertas categorías de productos tienen más problemas de logística que otras.

Las correlaciones entre variables se pueden investigar ejecutando consultas que combinan diferentes campos del dataset. Por ejemplo se puede analizar si los envíos a regiones más alejadas tienen más retrasos o si los tickets de soporte están relacionados con categorías de productos específicas.

La detección de anomalías se realiza identificando datos que se desvían del patrón esperado. Por ejemplo si una región tiene un porcentaje inusualmente alto de retrasos puede indicar un problema específico que requiere atención. Impala permite identificar estas anomalías rápidamente gracias a su velocidad de respuesta.

## Oozie

### Fundamentos Teóricos

Oozie es un sistema de orquestación de workflows diseñado para coordinar múltiples tareas en el ecosistema Hadoop. Permite definir flujos de trabajo que encadenan diferentes acciones como importación procesamiento y consulta. Oozie gestiona la ejecución de estas acciones incluyendo el manejo de errores y la reejecución de tareas fallidas.

Un workflow en Oozie es una secuencia de acciones definida en un archivo XML. Cada acción representa una tarea específica como ejecutar un job de MapReduce un script de Pig una consulta de Hive o un comando de Sqoop. Las acciones se ejecutan en el orden definido por el flujo del workflow.

Los nodos de acción en Oozie representan las tareas individuales del workflow. Cada nodo define el tipo de acción los parámetros necesarios y las transiciones hacia los siguientes nodos. Oozie soporta múltiples tipos de acciones incluyendo MapReduce Pig Hive Sqoop Java shell y HTTP.

Las transiciones en Oozie definen el flujo de ejecución entre acciones. Cada acción puede tener dos transiciones una para cuando la acción se completa exitosamente y otra para cuando falla. Esto permite crear flujos de trabajo robustos que manejan errores de manera apropiada.

La ejecución programada en Oozie permite definir cuándo se ejecutan los workflows. Se pueden programar ejecuciones únicas ejecuciones periódicas o ejecuciones basadas en eventos. Esta flexibilidad permite automatizar procesos que deben ejecutarse regularmente.

Oozie incluye un sistema de reintentos que permite reejecutar automáticamente acciones que fallan. Si una acción falla debido a un problema transitorio como un nodo temporalmente no disponible Oozie puede reintentar la acción después de un período de espera. Esto mejora la resiliencia de los flujos de trabajo.

### Aplicación Práctica

En el proyecto RetailTech Chile Oozie se utiliza para definir un workflow que encadena las principales etapas del proceso de datos. El workflow incluye la importación de datos con Sqoop el procesamiento con Pig y las consultas con Hive.

La definición del workflow comienza con la etiqueta workflow app que contiene el nombre del workflow. Se define un nodo start que indica el punto de inicio del flujo. A continuación se definen las acciones en el orden deseado.

La primera acción es un nodo de Sqoop que importa los datos desde una fuente externa. Se define la conexión a la base de datos la tabla a importar y el directorio de destino en HDFS. Si la importación es exitosa el flujo continúa con la siguiente acción. Si falla el flujo termina con un error.

La segunda acción es un nodo de Pig que procesa los datos importados. Se ejecuta un script que filtra agrupa y calcula métricas clave. Los resultados se guardan en un directorio de salida en HDFS.

La tercera acción es un nodo de Hive que ejecuta las consultas analíticas. Se ejecutan las siete consultas definidas en el proyecto y los resultados se almacenan para su posterior análisis.

Si todas las acciones se ejecutan exitosamente el workflow termina con un nodo end. Si alguna acción falla el workflow termina con un nodo kill que registra el error y notifica al administrador.

Para ejecutar el workflow se utiliza el comando "oozie job --oozie <http://localhost:11000/oozie> --config proyecto_retailtech --wf proyecto_retailtech_workflow.xml". Oozie monitorea la ejecución del workflow y reporta el estado de cada acción.

### Analítica de Datos

La automatización de reportes es uno de los principales beneficios de Oozie. Al definir un workflow que ejecuta las consultas de Hive periódicamente se generan reportes automáticamente sin intervención manual. Esto garantiza que los reportes estén siempre actualizados y disponibles para la toma de decisiones.

Los procesos batch para análisis periódico se benefician de la programación de Oozie. Por ejemplo se puede programar un workflow que se ejecute cada lunes por la mañana para generar un reporte semanal de ventas y logística. Los usuarios reciben el reporte actualizado sin tener que solicitarlo manualmente.

El control de calidad de datos se puede automatizar mediante Oozie. Se puede definir una acción que valide los datos después de cada importación y notifique al administrador si se encuentran problemas. Esto permite detectar y corregir errores de datos antes de que afecten los análisis.

La auditoría de procesos se facilita con Oozie ya que registra información sobre cada ejecución incluyendo la fecha de inicio la fecha de fin el estado de cada acción y los errores encontrados. Esta información es valiosa para el cumplimiento normativo y la mejora continua de los procesos.

## Flume

### Fundamentos Teóricos

Flume es un servicio distribuido diseñado para la ingesta de grandes volúmenes de datos en tiempo real. Fue desarrollado originalmente por Cloudera para recopilar datos de múltiples fuentes y transportarlos hacia destinos como HDFS o bases de datos. Flume es especialmente útil cuando se necesita ingesta continua de datos como logs sensores o eventos en tiempo real.

La arquitectura de Flume se basa en tres componentes principales fuente canal y destino. La fuente es el punto de origen de los datos puede ser un archivo de log un sensor una aplicación web o cualquier otra fuente de información. El canal es un buffer temporal que almacena los datos entre la fuente y el destino. El destino es donde se almacenan los datos finales típicamente HDFS.

Un agente en Flume es una instancia del proceso Flume que contiene la configuración de fuente canal y destino. Cada agente se ejecuta en un nodo del clúster y puede procesar datos de múltiples fuentes simultáneamente. Los agentes se configuran mediante archivos de propiedades que definen los componentes y sus relaciones.

Los canales en Flume actúan como buffers que absorben picos de tráfico. Cuando la fuente genera datos más rápido de lo que el destino puede procesar el canal almacena los datos temporalmente hasta que el destino esté listo para recibirlos. Esto garantiza que no se pierdan datos incluso durante períodos de alta carga.

Los tipos de fuente más comunes en Flume incluyen Avro Thrift exec spooldir y taildir. La fuente avro recibe datos de otros agentes Flume. La fuente exec ejecuta un comando y captura su salida. La fuente spooldir monitorea un directorio y procesa nuevos archivos que aparecen. La fuente taildir sigue archivos en tiempo real similar al comando tail de Linux.

Los destinos más comunes incluyen HDFS HBase y otros agentes Flume. El destino HDFS almacena los datos en el sistema de archivos distribuido. El destino HBase almacena datos en una base de datos NoSQL. Los agentes Flume pueden encadenarse para crear flujos de datos complejos.

### Aplicación Práctica

En el proyecto RetailTech Chile Flume se utiliza para demostrar la ingesta de datos en tiempo real. Se configura un agente que monitorea una carpeta local en busca de archivos CSV y los transporta hacia HDFS.

La configuración del agente se define en el archivo flume conf. Primero se define el nombre del agente con agent nombre. Luego se definen los componentes fuente canal y destino para el agente.

La fuente se configura como tipo taildir que monitorea archivos en tiempo real. Se define el grupo de archivos a monitorear con filegroups. Se especifica la ruta del directorio a monitorear y el patrón de archivos a seguir. En este caso se monitorea la carpeta donde se depositan los archivos CSV de ventas.

El canal se configura como tipo file channel que almacena los datos en disco. Este tipo de canal es más confiable que memoria channel ya que persiste los datos en caso de fallo del agente. Se define la ubicación del directorio donde se almacenan los datos del canal.

El destino se configura como tipo HDFS que almacena los datos en el sistema de archivos distribuido. Se define la ruta de destino en HDFS incluyendo variables de fecha para organizar los archivos por día. Se especifica el formato de archivos que es text y el prefijo de los archivos.

Para iniciar el agente se ejecuta el comando flume agent conf nombre archivo conf flume conf nombre agente. Flume comienza a monitorear la carpeta definida y transporta automáticamente cualquier archivo nuevo hacia HDFS.

Para probar la configuración se puede copiar un archivo CSV a la carpeta monitoreada y verificar que aparece en HDFS después de unos segundos. La verificación se realiza con hdfs dfs ls proyecto retailtech datos ingesta flume.

### Analítica de Datos

El monitoreo de datos entrantes permite conocer en tiempo real la cantidad y el tipo de información que ingresa al sistema. Flume proporciona métricas sobre la cantidad de eventos procesados el tamaño de los datos y la tasa de transferencia. Estas métricas son útiles para dimensionar los recursos del clúster.

La detección de anomalías en tiempo real se puede implementar configurando reglas de alerta en Flume. Por ejemplo si la cantidad de eventos supera un umbral esperado o si el tamaño de los archivos es inusualmente grande Flume puede generar alertas notificando al administrador.

Las alertas por patrones inusuales son importantes para detectar problemas de calidad de datos. Si un archivo CSV contiene un formato inesperado o campos faltantes Flume puede registrar advertencias que permiten identificar y corregir el problema antes de que afecte los análisis.

La integración con procesamiento batch se logra combinando Flume con otras herramientas del ecosistema. Mientras Flume ingesta datos en tiempo real herramientas como Pig o Hive procesan los datos periódicamente para generar reportes y análisis. Esta combinación permite tanto análisis en tiempo real como procesamiento histórico.

Conceptos Generales de Big Data

Las características del Big Data se definen mediante cinco dimensiones principales volumen velocidad variedad veracidad y valor. El volumen se refiere a la cantidad de datos que se procesan. En el proyecto RetailTech Chile el volumen es de 100 mil registros lo cual representa un conjunto de datos significativo para análisis. La velocidad indica la rapidez con la que se generan y procesan los datos. En un contexto retail los datos se generan continuamente a través de transacciones logística y soporte. La variedad describe los diferentes tipos de datos disponibles. El dataset de RetailTech incluye datos numéricos textuales y temporales. La veracidad se refiere a la confiabilidad de los datos. Los tests de validación ejecutados confirman que el dataset es confiable. El valor indica la utilidad de los datos para la toma de decisiones. Las consultas analíticas del proyecto generan información valiosa para los departamentos de ventas logística y soporte.

Las arquitecturas de almacenamiento distribuido resuelven las limitaciones del almacenamiento tradicional. Cuando los datos crecen más allá de lo que puede manejar un solo servidor se necesita distribuir la información entre múltiples máquinas. Hadoop implementa esta arquitectura mediante HDFS que divide los datos en bloques y los reparte entre los nodos del clúster. Esta aproximación permite escalar el almacenamiento añadiendo más nodos sin necesidad de hardware especializado.

Las herramientas del ecosistema Hadoop trabajan juntas para cubrir todas las necesidades del ciclo de vida de los datos. HDFS proporciona el almacenamiento. MapReduce y Pig ofrecen procesamiento. Hive e Impala permiten consultas SQL. Sqoop facilita la ingesta batch. Flume maneja la ingesta en tiempo real. Oozie orquesta los flujos de trabajo. Cada herramienta resuelve una parte específica del problema y se integra con las demás para crear una solución completa.

El procesamiento batch y el procesamiento en tiempo real son enfoques complementarios. El procesamiento batch se utiliza para analizar grandes volúmenes de datos acumulados. Es ideal para generación de reportes y análisis históricos. El procesamiento en tiempo real se utiliza para analizar datos a medida que llegan. Es ideal para monitoreo y detección de anomalías. En el proyecto RetailTech Chile el procesamiento batch se realiza con Hive y Pig mientras que Flume maneja la ingesta en tiempo real.

La calidad de datos es fundamental para obtener resultados confiables. Datos sucios incompletos o inconsistentes generan análisis incorrectos y decisiones erróneas. Las técnicas de limpieza de datos incluyen eliminación de duplicados corrección de errores imputación de valores faltantes y validación de formatos. En el proyecto se ejecutan tests de validación que verifican encoding delimitadores tipos de datos y consistencia lógica antes de proceder con el análisis.
