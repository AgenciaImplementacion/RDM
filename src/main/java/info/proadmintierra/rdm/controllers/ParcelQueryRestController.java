package info.proadmintierra.rdm.controllers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * ParcelQueryRestController
 */
@RestController
@RequestMapping("/query")
@CrossOrigin(origins = {"http://localhost:4200","*"})
public class ParcelQueryRestController {

    @GetMapping(value = "/parcel", produces = { "application/json" })
    public Map<String, Object> getParcelBasicInfo(@RequestParam(required = false) String nupre,
            @RequestParam(required = false) String cadastralCode, @RequestParam(required = false) String fmi) {

        try {
            File file = ResourceUtils.getFile("classpath:static/info_basica.json");

            InputStream inputStream = new FileInputStream(file);
            ObjectMapper mapper = new ObjectMapper();
            try {
                Map<String, Object> jsonMap = mapper.readValue(inputStream, Map.class);

                return jsonMap;

            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return null;

    }

   
}