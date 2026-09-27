package controller

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/cinvat/peretum/api/v1alpha1/service"
	"github.com/cinvat/peretum/internal/config"
)

// RegisterTargetsRoutes registers the target CRUD routes on the given router.
func RegisterTargetsRoutes(r *gin.Engine) {
	targets := r.Group("/v1alpha1/targets")
	{
		targets.GET("", listTargets)
		targets.POST("", createTarget)
		targets.PUT("/:server_name", updateTarget)
		targets.PATCH("/:server_name", patchTarget)
		targets.DELETE("/:server_name", deleteTarget)
	}
}

func listTargets(c *gin.Context) {
	targets, err := service.ListTargets()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Return a simplified view: just server_name + filename info.
	type targetSummary struct {
		ServerName string `json:"server_name"`
	}
	summaries := make([]targetSummary, 0, len(targets))
	for _, t := range targets {
		summaries = append(summaries, targetSummary{ServerName: t.ServerName})
	}
	c.JSON(http.StatusOK, summaries)
}

func createTarget(c *gin.Context) {
	var t config.TargetConfig
	if err := c.BindJSON(&t); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON: " + err.Error()})
		return
	}

	if err := service.CreateTarget(&t); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"server_name": t.ServerName, "message": "target created"})
}

func updateTarget(c *gin.Context) {
	serverName := c.Param("server_name")
	if serverName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "server_name parameter is required"})
		return
	}

	var t config.TargetConfig
	if err := c.BindJSON(&t); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON: " + err.Error()})
		return
	}

	if err := service.UpdateTarget(serverName, &t); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"server_name": t.ServerName, "message": "target updated"})
}

func deleteTarget(c *gin.Context) {
	serverName := c.Param("server_name")
	if serverName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "server_name parameter is required"})
		return
	}

	if err := service.DeleteTarget(serverName); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"server_name": serverName, "message": "target deleted"})
}

func patchTarget(c *gin.Context) {
	serverName := c.Param("server_name")
	if serverName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "server_name parameter is required"})
		return
	}

	data, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read body"})
		return
	}

	if err := service.PatchTarget(serverName, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"server_name": serverName, "message": "target patched"})
}
