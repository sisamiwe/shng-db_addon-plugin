DatabaseAddon
=============

Was macht das Plugin?
---------------------

Das Plugin bietet eine Funktionserweiterung zum Database Plugin und ermöglicht die einfache Auswertung von Messdaten.
Basierend auf den Daten in der Datenbank können bspw. Auswertungen zu Verbrauch (heute, gestern, ...) oder zu Minimal- und Maximalwerten gefahren werden.
Diese Auswertungen werden zyklisch zum Tageswechsel, Wochenwechsel, Monatswechsel oder Jahreswechsel, in Abhängigkeit der Funktion erzeugt.
Um die Zugriffe auf die Datenbank zu minimieren, werden diverse Daten zwischengespeichert.

Die Items mit einem DatabaseAddon-Attribut müssen im gleichen Pfad sein, wie das Item, für das das Database Attribut konfiguriert ist.
Bedeutet. Die Items mit dem DatabaseAddon-Attribute müssen Kinder oder Kindeskinder oder Kindeskinderkinder des Items sein, für das das Database Attribut konfiguriert ist
Bsp:

.. code-block:: yaml

    temperatur:
        type: bool
        database: yes

        auswertung:
            type: foo

            heute_min:
                type: num
                database_addon_fct: heute_min

            gestern_max:
                type: num
                database_addon_fct: heute_minus1_max


Anforderungen
-------------
Es muss das Database Plugin konfiguriert und aktiv sein. Die Konfiguration erfolgt automatisch bei Start.

Zudem sollten by Verwendung von mysql einige Variablen der Datenbank angepasst werden, so dass die komplexen Anfragen ohne Fehler bearbeitet werden.
Dazu folgenden Block am Ende der Datei */etc/mysql/my.cnf* einfügen bzw den existierenden ergänzen.

.. code-block:: bash

    [mysqld]
    connect_timeout = 60
    net_read_timeout = 60
    wait_timeout = 28800
    interactive_timeout = 28800


Konfiguration
-------------

plugin.yaml
^^^^^^^^^^^

Bitte die Dokumentation lesen, die aus den Metadaten der plugin.yaml erzeugt wurde.


items.yaml
^^^^^^^^^^

Bitte die Dokumentation lesen, die aus den Metadaten der plugin.yaml erzeugt wurde.


logic.yaml
^^^^^^^^^^

Bitte die Dokumentation lesen, die aus den Metadaten der plugin.yaml erzeugt wurde.


Funktionen
^^^^^^^^^^

Bitte die Dokumentation lesen, die aus den Metadaten der plugin.yaml erzeugt wurde.


Beispiele
---------

Hier können ausführlichere Beispiele und Anwendungsfälle beschrieben werden.


Web Interface
-------------

Das WebIF stellt neben der Ansicht verbundener Items und deren Parameter und Werte auch Funktionen für die
Administration des Plugins bereit.

Es stehen Button für:

- Neuberechnung aller Items
- Abbruch eines aktiven Berechnungslaufes
- Pausieren des Plugins
- Wiederaufnahme des Plugins

bereit.

Achtung: Das Auslösen einer kompletten Neuberechnung aller Items kann zu einer starken Belastung der Datenbank
aufgrund vieler Leseanfragen führen.


DatabaseAddOn Items
^^^^^^^^^^^^^^^^^^^

Dieser Reiter des Webinterface zeigt die Items an, für die ein DatabaseAddon Attribut konfiguriert ist.

.. image:: user_doc/assets/webif_tab1.jpg
   :class: screenshot

DatabaseAddOn Maintenance
^^^^^^^^^^^^^^^^^^^^^^^^^

Das Webinterface zeigt detaillierte Informationen über die im Plugin verfügbaren Daten an.
Dies dient der Maintenance bzw. Fehlersuche. Dieser Tab ist nur bei Log-Level "Debug" verfügbar.

.. image:: user_doc/assets/webif_tab2.jpg
   :class: screenshot
