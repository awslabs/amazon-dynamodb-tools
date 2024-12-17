package com.dynamodbdemo.controller;

import com.dynamodbdemo.model.auth.RequestDTO;
import com.dynamodbdemo.service.BL.EntityRecordReadServiceBL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/ddbDemo/")
public class DemoRestController {


    @Qualifier("EntityRecordReadServiceDDbNativeHedgingImpl")
    @Autowired()
    private EntityRecordReadServiceBL entityRecordReadServiceBLHedging;


    @Qualifier("EntityRecordReadServiceDDbNativeImpl")
    @Autowired()
    private EntityRecordReadServiceBL entityRecordReadServiceBL;

    @PostMapping("readDataWithHedging")
    public RequestDTO readDataWithHedging(@RequestBody RequestDTO requestDTO) throws Exception {
        RequestDTO requestDTOResponse = entityRecordReadServiceBLHedging.transactRecords(requestDTO);
        requestDTOResponse.setTransId(Thread.currentThread().toString());
        return requestDTOResponse;
    }

    @PostMapping("readData")
    public RequestDTO readData(@RequestBody RequestDTO requestDTO) throws Exception {
        RequestDTO requestDTOResponse = entityRecordReadServiceBL.transactRecords(requestDTO);
        requestDTOResponse.setTransId(Thread.currentThread().toString());
        return requestDTOResponse;
    }

}
