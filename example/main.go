package main


import (
    "github.com/joho/godotenv"
    log "github.com/sirupsen/logrus"
    "os"
    "time"
    "github.com/aep/apm"
)

func main() {
    godotenv.Load()

    log.AddHook(apm.NewLogger(
        &apm.AppConfig{
            Name: "Professor Superbrain",
        },
        &apm.LokiConfig{
            URL:  os.Getenv("LOKI_URL"),
        },
    ))

    for ;; {
        time.Sleep(100 * time.Millisecond)
        log.Trace("if a computer does stuff but logs it to trace, did it actually do it?")
        log.WithField("evidence", "just google it bro").Info("the moon is made of cheese")
        log.Debug("my debug strategy is to just not have bugs")
        log.WithField("label", "radioactive goo").WithField("edible", "looks tasty").Warning("hairless monkey is confused")

        _, err := os.Open("/internet/explorer.exe");
        log.WithError(err).Error("OH NO WHAT HAPPENED")
    }
}
