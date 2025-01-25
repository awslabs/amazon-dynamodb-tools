package com.dynamodbdemo.service.BL;

import com.dynamodbdemo.model.RequestDTO;

public interface EntityRecordReadServiceBL {

    RequestDTO transactRecords(RequestDTO requestDTO) throws Exception;

}
