package it.polimi.ds.chat.externalservices;

public class DirectoryService {
    /*
    directoryservice conosce tutti i broker poichè all'entrata nella rete contattano il suddetto,
    ha una lista di broker e client connessi ad ognuno
    costui continua a pingare tutti i brokers e al ricevere di una richiesta di connessione
    smista i client in base al numero di connessioni ad ogni broker (il broker deve notificare
    quando un client si disconnette ed al corretto reggiungimento di una connessione per incrementare e decrementare)
    se dopo N ping il server non risponde lo rimuove dalla lista

    in caso di crash tutti i client ricontatteranno in automatico questa directory service per
    riconnettersi ad un altro broker
     */
}
