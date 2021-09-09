package xtdb.api.query

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import xtdb.api.XtdbDocument
import xtdb.api.XtdbKt
import xtdb.api.query.conversion.q
import xtdb.api.query.domain.XtdbDocumentSerde
import xtdb.api.tx.submitTx
import xtdb.api.underware.kw
import xtdb.api.underware.sym
import java.time.Duration
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SerdeQueryTest {
    companion object {
        private data class Person(val id: String, val forename: String, val surname: String)
        private data class Vehicle(val id: String, val make: String, val owner: String)
        private data class Journey(val id: String, val city: String, val vehicle: String)

        private object XtdbPerson : XtdbDocumentSerde<Person> {
            override fun toDocument(obj: Person): XtdbDocument =
                XtdbDocument.build(obj.id) {
                    it.put("forename", obj.forename)
                    it.put("surname", obj.surname)
                }

            override fun toObject(document: XtdbDocument): Person =
                Person(
                    document.id as String,
                    document.get("forename") as String,
                    document.get("surname") as String
                )
        }

        private object XtdbVehicle : XtdbDocumentSerde<Vehicle> {
            override fun toDocument(obj: Vehicle): XtdbDocument =
                XtdbDocument.build(obj.id) {
                    it.put("make", obj.make)
                    it.put("owner", obj.owner)
                }

            override fun toObject(document: XtdbDocument): Vehicle =
                Vehicle(
                    document.id as String,
                    document.get("make") as String,
                    document.get("owner") as String
                )
        }

        private object XtdbJourney : XtdbDocumentSerde<Journey> {
            override fun toDocument(obj: Journey): XtdbDocument =
                XtdbDocument.build(obj.id) {
                    it.put("city", obj.city)
                    it.put("vehicle", obj.vehicle)
                }

            override fun toObject(document: XtdbDocument): Journey =
                Journey(
                    document.id as String,
                    document.get("city") as String,
                    document.get("vehicle") as String
                )
        }

        private val person = "person".sym
        private val vehicle = "vehicle".sym
        private val journey = "journey".sym

        private val owner = "owner".kw
        private val forename = "forename".kw
        private val transport = "vehicle".kw
        private val city = "city".kw
    }

    private fun person(forename: String, surname: String) =
        Person(UUID.randomUUID().toString(), forename, surname)

    private fun vehicle(make: String, owner: String) =
        Vehicle(UUID.randomUUID().toString(), make, owner)

    private fun journey(city: String, vehicle: String) =
        Journey(UUID.randomUUID().toString(), city, vehicle)

    private val tom = person("Thomas", "Moore")
    private val richard = person("Richard", "Starkey")
    private val harry = person("Harry", "Potter")

    private val people = setOf(tom, richard, harry)

    private val tourBus = vehicle("Volkswagen Kombi", richard.id)
    private val bentley = vehicle("Bentley", richard.id)
    private val flyingCar = vehicle("Ford Anglia", harry.id)

    private val vehicles = setOf(tourBus, bentley, flyingCar)

    private val tourJourneys = setOf("Edinburgh", "Paris", "Rome", "London", "Berlin")
        .map { journey(it, tourBus.id) }

    private val missedTheTrain = journey("Edinburgh", flyingCar.id)

    private val casualRide = journey("Banbury", bentley.id)

    private val journeys = tourJourneys + missedTheTrain + casualRide


    private val db = XtdbKt.startNode().apply {
        submitTx {
            people.forEach { person ->
                put(person by XtdbPerson)
            }

            vehicles.forEach { vehicle ->
                put(vehicle by XtdbVehicle)
            }

            journeys.forEach { journey ->
                put(journey by XtdbJourney)
            }
        }.also {
            awaitTx(it, Duration.ofSeconds(10))
        }
    }.db()

    @Test
    fun `can get all people`() =
        assertThat(
            db.q<Person>(XtdbPerson) {

                find(person)

                where {
                    person has forename
                }
            }.toSet(),

            equalTo(people)
        )

    @Test
    fun `can get all vehicles owned by a person`() =
        assertThat(
            db.q<Vehicle>(XtdbVehicle) {
                find(vehicle)

                where {
                    vehicle has owner eq person
                    person has forename eq "Richard"
                }
            }.toSet(),
            equalTo(setOf(tourBus, bentley))
        )

    @Test
    fun `can get vehicles matched to owner`() =
        assertThat(
            db.q<Person, Vehicle>(XtdbPerson, XtdbVehicle) {
                find(person, vehicle)

                where {
                    vehicle has owner eq person
                }
            }.toSet(),
            equalTo(
                setOf(
                    richard to tourBus,
                    richard to bentley,
                    harry to flyingCar
                )
            )
        )

    @Test
    fun `can get journeys matched to vehicle and owner`() =
        assertThat(
            db.q<Person, Vehicle, Journey>(XtdbPerson, XtdbVehicle, XtdbJourney) {
                find(person, vehicle, journey)

                where {
                    vehicle has owner eq person
                    journey has transport eq vehicle
                    journey has city eq "Edinburgh"
                }
            }.toSet(),
            equalTo(
                setOf(
                    Triple(harry, flyingCar, missedTheTrain),
                    Triple(richard, tourBus, tourJourneys[0])
                )
            )
        )
}