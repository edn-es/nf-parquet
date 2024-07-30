include {
        writeRecords
} from 'plugin/nf-parquet'

import myrecords.*

writeRecords(
        "presidents.parquet",
        [
                new Person(1010101,
                        new Job("USA", "POTUS", 3),
                        new Address("1600 Pennsylvania Av.", "20500", "Washington")),
                new Person(1010102,
                        new Job("Spain", "SOTUS", 12),
                        new Address("Moncloa.", "28000", "Madrid")),
        ])
