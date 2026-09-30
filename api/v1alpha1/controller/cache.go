package controller

import (
	"net/http"

	"github.com/cinvat/peretum/api/v1alpha1/service"
	"github.com/gin-gonic/gin"
)

// PurgeCache removes cached entries from the disk cache:
//
//	DELETE /cache?host=example.com                  — whole site
//	DELETE /cache?host=example.com&path=/exact      — single object
//	DELETE /cache?host=example.com&prefix=/loc/     — location subtree
//	DELETE /cache?all=true                          — whole cache
//
// In standalone mode it purges local cache files and reports how many
// entries were removed. In cluster mode (--nats-uri) it publishes the
// purge to every edge and reports {"published": true} without per-edge
// counts.
func PurgeCache(c *gin.Context) {
	host := c.Query("host")
	path := c.Query("path")
	prefix := c.Query("prefix")
	all := c.Query("all") == "true"

	result, err := service.PurgeCache(host, path, prefix, all)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}
