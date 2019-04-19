package com.yahoo.bard.webservice.util

import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.table.availability.Availability
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils
import com.yahoo.bard.webservice.table.availability.TimeFilteredAvailability
import com.yahoo.bard.webservice.table.physicaltables.PureUnionPhysicalTable
import com.yahoo.bard.webservice.table.physicaltables.StrictPhysicalTable
import com.yahoo.bard.webservice.table.physicaltables.TimeFilteredPhysicalTable

import org.joda.time.Interval

import java.util.function.Supplier

class ClassScannerSpecHelper {

    ClassScanner scanner

    ClassScannerSpecHelper(ClassScanner scanner) {
        this.scanner = scanner;
    }

    TableName tableName = TableName.of("foo")
    Supplier<Interval> intervalSupplier = (Supplier) { new Interval("2000/P2Y") }
    Supplier<SimplifiedIntervalList> intervalListSupplier = (Supplier) {
        new SimplifiedIntervalList([new Interval("2000/P2Y")])
    }
    Availability availability = new AvailabilityTestingUtils.TestAvailability([] as Set, [:])


    Optional<Object> constructSpecial(Class clazz) {
        if (clazz == PureUnionPhysicalTable.class) {
            Object table = scanner.constructObject(StrictPhysicalTable.class, ClassScanner.Args.VALUES)
            Object o = new PureUnionPhysicalTable(tableName, [table] as Set)
            return Optional.of(o)
        }
        if (clazz == TimeFilteredAvailability.class) {
            Object availability = new AvailabilityTestingUtils.TestAvailability([] as Set, [:])
            return Optional.of(new TimeFilteredAvailability(availability, intervalSupplier))
        }
        if (clazz == TimeFilteredPhysicalTable.class) {
            return Optional.of(new TimeFilteredPhysicalTable(
                    tableName,
                    scanner.constructObject(StrictPhysicalTable.class, ClassScanner.Args.VALUES),
                    intervalListSupplier
            ))
        }
        return Optional.empty()
    }
}
