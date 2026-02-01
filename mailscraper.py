import sys
try:
    # # ======================================================================= # #
    # #  Python-Script created by: Jonathan Kaufmann                            # #
    # #                                                                         # #
    # #  Last changes:                 | Date / Modificator:                    # #
    # #  ------------------------------+--------------------------------------  # #
    # #  Create File and implement it  | 23.03.2024 / Jonathan                  # #
    # #                                |                                        # #
    # #                                                                         # #
    # # ======================================================================= # #

    # # ----------------------------------------------------------------------- # #
    # #  Dieses Python-Skript ruft über die API-Schnittstelle LINDAS vom Bund   # #
    # #  mit SPARQL-Befehlen die Hydrodaten sowie über die API-Schnittstelle    # #
    # #  von SRF-Meteo die Wetterdaten ab und gibt diese bereinigt aus.         # #
    # #                                                                         # #
    # # ----------------------------------------------------------------------- # #


    import os                                # zum lesen aller Konfigurationen und schreiben aller Ergebnisse
    import json                              # zum lesen aller Konfigurationen und schreiben aller Ergebnisse
    import imaplib                           # IMAP-Abfrage
    import email                             # IMAP-Abfrage
    from email.header import decode_header   # Email-Betreff abfragen
    from datetime import datetime            # Für Timestamp als Metadaten
    import time                              # Warten...


    ## Parameter definieren
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    imap_configfile = './imap.conf'
    scraping_config_path = './config/'

    exportpath = './data/'


    ## IMAP-Konfigurationen lesen
    with open(imap_configfile, 'r') as file:
        content = file.read()
        imap_config = json.loads(content)


    ## Emailfilter-Konfigurationen lesen
    def read_email_configs(directory: str, fileending: str) -> dict:
        data = {}

        for filename in os.listdir(directory):
            if filename.endswith(fileending):
                file_path = os.path.join(directory, filename)
                with open(file_path, 'r', encoding='utf-8') as file:
                    content = file.read()
                    key = os.path.splitext(filename)[0]
                    data[key] = json.loads(content)

        return data


    ## Posteingang nach neuen Mails mit bestimmten Suchfiltern durchsuchen (gibt den Body der Mail zurück)
    def check_for_new_email(imap_config: dict, subjectfilter: str = None) -> str:
        #  Verbindung zum IMAP-Server herstellen
        mail = imaplib.IMAP4_SSL(imap_config['imap_server'], imap_config['imap_port'])

        try:
            mail.login(imap_config['username'], imap_config['password'])
            mail.select('inbox', readonly=False)

            #  Suche nach ungelesenen E-Mails
            status, messages = mail.search(None, 'UNSEEN')

            if len(messages[0]) == 0:
                print(f'Keine ungelesenen E-Mails.')
                return None

            #  Nachrichten in einer Liste aufteilen und nacheinander auslesen
            message_ids = messages[0].split()

            for msg_id in message_ids:
                status, msg_data = mail.fetch(msg_id, '(RFC822)')

                for response_part in msg_data:
                    if isinstance(response_part, tuple):

                        #  Nachricht und Betreff dekodieren
                        msg = email.message_from_bytes(response_part[1])

                        subject, encoding = decode_header(msg['Subject'])[0]
                        if isinstance(subject, bytes):
                            subject = subject.decode(encoding if encoding else 'utf-8')

                        #  Prüfen, ob der Betreff mit dem Suchkriterium übereinstimmt
                        if (subjectfilter!= None and subject == subjectfilter):
                            print(f'Neue E-Mail gefunden: {subject} (Message-ID {int(msg_id)})')

                            #  Inhalt der E-Mail verarbeiten
                            body = msg.get_payload(decode=True)
                            charset = msg.get_content_charset() or "utf-8"

                            try:
                                bodytext = body.decode(charset, errors="replace")
                            except Exception:
                                bodytext = body.decode("utf-8", errors="replace")
                            
                            #  E-Mail-Text zurückgeben und Suche nach Emails abschliessen
                            mail.uid('STORE', msg_id, '+FLAGS', '\\seen')
                            return bodytext 
                        
                        #  Wenn E-Mail nicht dem Betreff entspricht, wieder als ungelesen markieren
                        else:
                            mail.uid('STORE', msg_id, '-FLAGS', '\\seen')
                            
        except Exception as err:
            print(f'Fehler beim durchsuchen der Emails: {str(err)}')
            return None
        finally:
            mail.logout()
    

    ## Extrahiere Elemente und Attribute aus Emailtext
    def extract_bodydata(mailbody: str, elementseparator: str, attributes: dict) -> list[dict]:
        result = []

        try:
            #  Elemente aufteilen
            elements = mailbody.split(elementseparator)
            for element in elements[1:]:
                json_element = {}

                #  Zeile für Zeile des Elements auslesen
                lines = [l.strip() for l in element.splitlines() if l.strip()]
                for attributename, attributeconfig in attributes.items():

                    #  Wert für Resultat zusammenstellen
                    value = lines[attributeconfig['liniennummer']]
                    if attributeconfig.get('replacetext'):
                        value = value.replace(attributeconfig['replacetext'], '')

                    json_element[attributename] = value

                #  Daten sammeln und zurückgeben
                result.append(json_element)
            return result
    
        except Exception as err:
            print(f'Fehler beim auswerten des E-Mail-Inhalts: {str(err)}')
            return None


    ## Speichere die abgerufenen Daten als sauberes JSON
    def savedata_json(filename: str, content: dict, encoding: str = 'utf-8') -> None:
        filepath = os.path.join(exportpath, filename+'.json')
        if os.path.exists(filepath):
            os.remove(filepath)
        with open(filepath, 'w', encoding=encoding) as file:
            json.dump(content, file, indent=4, ensure_ascii=False)

        print(f' -> Daten in {filepath} gespeichert')


    ## Führt alle Funktionen aus
    def main() -> None:
        email_configs = read_email_configs(scraping_config_path, 'json')

        for configtitle, mailconfig in email_configs.items():
            print(f'Überprüfe Posteingang für {configtitle} (Betreff "{mailconfig["suchfilter"]["betreff"]}")')

            #  Suche E-Mail in Postfach
            mailcontent = check_for_new_email(imap_config, subjectfilter=mailconfig['suchfilter']['betreff'])
            
            if not mailcontent:
                #print(f'keine Übereinstimmung mit diesen Suchkriterien, überspringe "{configtitle}"')
                continue
            
            #  gelesenes E-Mail auswerten und JSON abspeichern
            jsondata = extract_bodydata(mailcontent, mailconfig['elementseparator'], mailconfig['exportattribute'])
            if jsondata:
                metadata = {
                    "config": configtitle,
                    "export_timestamp": str(datetime.now())
                }
                savedata_json(configtitle, {'metadata': metadata, 'data': jsondata})


    ## Initiator für main()
    if __name__ == "__main__":
        main()
        
        # while True:
        #     main()
        #     print('Warte 60 Sekunden...')
        #     print()
        #     time.sleep(60)


except Exception as err:
    import traceback
    print(traceback.format_exc())
    sys.exit(1)
else:
    sys.exit(0)