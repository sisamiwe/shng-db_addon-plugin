DatabaseAddon
=============

Was macht das Plugin?
---------------------

Das Plugin bietet eine Funktionserweiterung zum Database Plugin und ermöglicht somit die einfache Auswertung von Messdaten.
Basierend auf den Daten in der Datenbank können bspw. Auswertungen zu Verbrauch (heute, gestern, ...) oder Auswertungen zu Minimal- und Maximalwerten gefahren werden.
Diese Auswertungen werden zyklisch zum Tageswechsel, Wochenwechsel, Monatswechsel oder Jahreswechsel erzeugt.
Um die Zugriffe auf die Datenbank zu minimieren, werden diverse Daten zwischengespeichert.

Die Items mit einem DatabaseAddon-Attribut müssen im gleichen Pfad sein, wie das Item, für das das Database Attribut konfiguriert ist.
Bedeutet. Die Items mit dem DatabaseAddon-Attribute müssen im Kinder oder Kindeskinder oder Kindeskinderkinder des Items sein, für das das Database Attribut konfiguriert ist
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

Ein KLick auf den Button "Recalc all" stößt die Berechnung aller Items, für die ein Attribut gesetzt ist, an.
Achtung: Das kann zu einer starken Belastung der Datenbank aufgrund vieler Leseanfragen führen.

DatabaseAddOn Items
^^^^^^^^^^^^^^^^^^^

Dieser Reiter des Webinterface zeigt die Items an, für die ein DatabaseAddon Attribut konfiguriert ist.

.. image:: user_doc/assets/webif_tab1.jpg
   :class: screenshot

DatabaseAddOn Maintenance
^^^^^^^^^^^^^^^^^^^^^^^^^

Das Webinterface zeigt detaillierte Informationen über die im Plugin verfügbaren Daten an.
Dies dient der Maintenance bzw. Fehlersuche.

.. image:: user_doc/assets/webif_tab2.jpg
   :class: screenshot
