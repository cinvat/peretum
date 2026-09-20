package waf

import (
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/oschwald/maxminddb-golang"
	"k8s.io/klog/v2"
)

// geodb bundles the GeoLite2 Country, ASN and optional City databases.
// The Geo databases are general: a single shared directory holds them and
// every per-location WAF policy reads from the same readers.
type geodb struct {
	country *maxminddb.Reader
	asn     *maxminddb.Reader
	city    *maxminddb.Reader
}

type countryRecord struct {
	Country struct {
		ISOCode string `maxminddb:"iso_code"`
	} `maxminddb:"country"`
	RegisteredCountry struct {
		ISOCode string `maxminddb:"iso_code"`
	} `maxminddb:"registered_country"`
}

type asnRecord struct {
	ASN uint32 `maxminddb:"autonomous_system_number"`
	Org string `maxminddb:"autonomous_system_organization"`
}

type cityRecord struct {
	City struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
}

// openGeodb opens the GeoLite databases inside dir. The Country and ASN
// databases are required; the City database is optional (returns empty city
// names when absent). Missing files degrade to empty results rather than
// failing the whole server.
func openGeodb(dir string) (*geodb, error) {
	if dir == "" {
		return &geodb{}, nil
	}
	g := &geodb{}
	if r, err := openReader(filepath.Join(dir, "GeoLite2-Country.mmdb")); err != nil {
		return nil, err
	} else {
		g.country = r
	}
	if r, err := openReader(filepath.Join(dir, "GeoLite2-ASN.mmdb")); err != nil {
		klog.Warningf("waf: ASN database not loaded: %v", err)
	} else {
		g.asn = r
	}
	if r, err := openReader(filepath.Join(dir, "GeoLite2-City.mmdb")); err != nil {
		if !os.IsNotExist(err) {
			klog.Warningf("waf: city database not loaded: %v", err)
		}
	} else {
		g.city = r
	}
	return g, nil
}

func openReader(path string) (*maxminddb.Reader, error) {
	r, err := maxminddb.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	return r, nil
}

// lookup returns the country ISO code, ASN number, ASN organization and city
// name for the given IP. Any component whose database is unavailable returns
// its zero value.
func (g *geodb) lookup(ip net.IP) (country string, asn uint32, asnOrg, city string) {
	if g.country != nil {
		var rec countryRecord
		if err := g.country.Lookup(ip, &rec); err == nil {
			country = rec.Country.ISOCode
			if country == "" {
				country = rec.RegisteredCountry.ISOCode
			}
		}
	}
	if g.asn != nil {
		var rec asnRecord
		if err := g.asn.Lookup(ip, &rec); err == nil {
			asn = rec.ASN
			asnOrg = rec.Org
		}
	}
	if g.city != nil {
		var rec cityRecord
		if err := g.city.Lookup(ip, &rec); err == nil {
			city = rec.City.Names["en"]
		}
	}
	return
}

func (g *geodb) close() {
	if g.country != nil {
		_ = g.country.Close()
	}
	if g.asn != nil {
		_ = g.asn.Close()
	}
	if g.city != nil {
		_ = g.city.Close()
	}
}
