package com.dynamodbdemo.service.Impl;

import com.dynamodbdemo.dao.EntityRecordDDbNativeDAO;
import com.dynamodbdemo.model.auth.DDBMetaDataAccessor;
import com.dynamodbdemo.model.auth.DDBResponse;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service("EntityRecordReadServiceDDbNativeImpl")
public class EntityRecordReadServiceDDbNativeImpl extends AbstractEntityRecordReadServiceImpl {

    private final EntityRecordDDbNativeDAO entityRecordDDbNativeDAO;

    public EntityRecordReadServiceDDbNativeImpl(EntityRecordDDbNativeDAO entityRecordDDbNativeDAO) {
        this.entityRecordDDbNativeDAO = entityRecordDDbNativeDAO;
    }

    @Override
    public List<DDBMetaDataAccessor> getEntityRecords(String recordId, String entityNumber, int delayInMillis) throws InterruptedException {

        long startTime = System.currentTimeMillis();


        DDBResponse fetchByClientIDAndAppNumResponse = entityRecordDDbNativeDAO.fetchByRecordIDAndEntityNumber(recordId, entityNumber);


        long endTime = System.currentTimeMillis();
        fetchByClientIDAndAppNumResponse.setActualLatency(endTime - startTime);

        List<DDBMetaDataAccessor> metaDataAccessors = new ArrayList<>();
        metaDataAccessors.add(fetchByClientIDAndAppNumResponse);

        return metaDataAccessors;
    }
}
