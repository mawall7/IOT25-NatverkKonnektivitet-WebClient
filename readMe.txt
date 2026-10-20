- REST API beskrivning kolla nedan 

- SOA UA -client: obs inte testad ännu!
    TODO behöver fixas:
    - Säkerställa att bakgrundsuppgiften startas på ett kontrollerat sätt när webbappen startar.

    - Lägga till kontrollerad nedstängning där OPC UA-klienterna får möjlighet att koppla ned.


Version 1 Kod beskrivning - ej fullständig version

klart 1.klienten ansluter till flera OPC UA-servrar.

2.På varje server prenumererar klienten på de noder den är intresserad av, till exempel Device.DeviceId och olika mätvärden.

3.När ett värde ändras skickar OPC UA-servern en dataändringsnotifiering till klienten.

4.Python tar emot ändringen och skickar det nya värdet vidare via REST.

5. Om en server kopplas ned fortsätter de andra prenumerationerna att fungera, och klienten försöker återansluta till den server som tappats.


Version 2. KOD med återanslutning och  opc nod subscription handling 

Vid uppstart: Python startar en bakgrundsuppgift för varje server.

Vid anslutning: Varje server får en egen subscription på dina tre NodeId:er.

När värden ändras: datachange_notification() uppdaterar latest_values.

När REST-klienten frågar: GET /api/servers returnerar de senast mottagna värdena.

Om en server inte kan nås vid uppstart: Den serverns uppgift försöker igen efter fem sekunder. De övriga fortsätter.

Om en ansluten server tappar kontakten: auto_reconnect=True låter asyncua försöka återställa anslutningen och aktiva subscriptions.

Version 3 - grundstrukturen är rätt för att bygga en OPC UA-klient som ansluter till flera fjärrservrar, prenumererar på nodförändringar och gör de senaste värdena tillgängliga via ett REST API med HTTP GET.

Det finns dock några viktiga saker att rätta till: för att lösningen ska fungera tillförlitligt i drift, särskilt vid återanslutning, felhantering och hantering av data.
 övervaka flera OPC UA-servrar samtidigt. Med prenumerationer behöver klienten inte läsa alla noder kontinuerligt med polling; servern skickar i stället dataändringar enligt OPC UA:s prenumerationsmekanism.

2. Det som är rätt i koden
Flera fjärrservrar: asyncio.gather() kör en separat anslutningsloop för varje server.

DataChange-prenumerationer: create_subscription() och subscribe_data_change() används för att ta emot ändringar.


Trådsäker åtkomst: threading.Lock() skyddar läsning och skrivning mellan OPC UA-tråden och Flask.

Återförsök: Varje server har en egen loop med fem sekunders väntetid efter ett fel.

Icke-blockerande arkitektur: OPC UA-arbetet körs i en separat tråd från Flask-servern.


Problem som bör åtgärdas : återanslutning 
- vid initial anslutning hämtas värden - Fixat!
- vid initial anslutning görs retry 
- om någon server går ner efter initial anslutning sköts av automatiskt auto_reconnect
OCH då körs Inte connection.disconnect() !

gör om koden så att den skiljer mellan två situationer:

Första anslutningen misslyckas: klienten väntar fem sekunder och försöker igen.

Anslutningen bryts efter att den har etablerats: asyncua 2.1.0 får hantera den automatiska återanslutningen utan att vår kod i onödan anropar disconnect() och avbryter den.

Jag behåller också initial läsning av noderna, prenumerationer på dataändringar och REST API:t. Varje OPC UA-server hanteras oberoende av de andra.

I koden nedan använder vi även connection_lost_callback för att markera servern som frånkopplad. Om anslutningen återställs men inga nya nodvärden rapporteras kan statusen annars ligga kvar som False tills nästa dataändring.

Problem: återanslutning 2-Fixat Version 4 - måste testas:


Det här är den avsedda logiken:

Första anslutningen misslyckas: klienten städas upp och ett nytt försök görs efter fem sekunder.

Tillfälligt avbrott efter anslutning: koden anropar inte själv disconnect(); asyncua får hantera återanslutningen.

Prenumerationen etableras inte: koden försöker skapa den igen med samma klient.

Oväntat fel som avslutar klientens aktiva arbetsloop: klienten stängs och en ny anslutningscykel startas.

En reservation: den här koden bygger på att asyncua 2.1.0 hanterar återställningen av den aktiva prenumerationen i det aktuella avbrottsscenariot. Det är viktigt att testa serveromstart och nätverksavbrott i din miljö. Ingen kod kan garantera återställning om exempelvis servern har tappat sessionen och biblioteket inte kan återställa den automatiskt.



--------------------------------------------
GET API

REST API: /api/servers returnerar JSON via HTTP GET.

Vad jag skulle göra i ditt fall:
Jag skulle börja enkelt:
Så här funkar backend API GET just nu! 

Ett gemensamt GET /api/servers för samtliga sex OPC UA-servrar.

OPC UA-subscriptions uppdaterar värdena i bakgrunden.

Displayklienten hämtar data varannan sekund.

Klienten jämför värdena och uppdaterar displayen bara när något faktiskt har ändrats.

svaret kommer se ut så här: 

{
  "server1": {
    "connected": true,
    "last_update": "2026-10-20 17:41:23",
    "Data": {
      "DeviceId": "amid-esp32",
      "Humidity": 23.7,
      "Temperature" : 18.1,
      
      
    }
  },
  "server2": {
    "connected": true,
    "last_update": "2026-10-20 17:41:23",
    "Data": {
      "DeviceId": "hassan-esp32",
      "Humidity": 18.4,
      "Temperature: 20.0",
      
    }
  },
  "server3": {
    osv.. till server6
    }
  }

-Kan göras senare: Om du senare märker att trafiken är onödigt stor kan du lägga till ETag och 304 Not Modified. Du behöver inte optimera det från början.