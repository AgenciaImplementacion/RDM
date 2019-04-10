package info.proadmintierra.rdm.controllers;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * MainQueryRestController
 */
@RestController
@RequestMapping("/")
@CrossOrigin(origins = {"http://localhost:4200","*"})
public class MainQueryRestController {

    @GetMapping(value = "/", produces = { "text/html" })
    public String getHome() {
        String html="<html>";
        html += "<head><link href='https://fonts.googleapis.com/css?family=Allerta+Stencil' rel='stylesheet'>";
        html += "<title>RDM</title>";
        html += "<style>*{font-family: 'Allerta Stencil', sans-serif;}</style>";
        html += "</head>";
        html += "<body><center><div style='margin-top:40vh;font-size:80px;'>RDM</div></center></body>";
        html += "</html>";
        return html;

    }

   
}