package com.dynamodbdemo.service.BL;

import com.dynamodbdemo.model.auth.RequestDTO;

public interface EntityRecordReadServiceBL {

    RequestDTO transactRecords(RequestDTO requestDTO) throws Exception;

}
